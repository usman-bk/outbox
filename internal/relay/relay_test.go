package relay_test

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/usmanvirk/outbox/internal/outbox"
	"github.com/usmanvirk/outbox/internal/relay"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// A real database or nothing. What is under test is SKIP LOCKED, a
// transaction held across a network call, and what a rollback does to a
// claimed row, and a mock cannot have any of those properties.
func fresh(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL is not set")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	if _, err := pool.Exec(ctx, outbox.Schema); err != nil {
		t.Fatalf("schema: %v", err)
	}
	// The orders table too. It is created by the first test that needs it
	// and shared by the rest, so leaving rows behind makes the second test
	// to use an id collide on the primary key.
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS orders (id text PRIMARY KEY)`); err != nil {
		t.Fatalf("orders: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE outbox, orders`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return pool
}

// recorder is a Publisher that counts, and can be told to fail.
type recorder struct {
	mu       sync.Mutex
	messages []string
	fail     atomic.Bool
	delay    time.Duration
	calls    atomic.Int64
}

func (r *recorder) Publish(ctx context.Context, topic, key string, payload []byte) error {
	r.calls.Add(1)
	if r.delay > 0 {
		select {
		case <-time.After(r.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if r.fail.Load() {
		return errors.New("broker is down")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, fmt.Sprintf("%s/%s/%s", topic, key, payload))
	return nil
}

func (r *recorder) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.messages))
	copy(out, r.messages)
	return out
}

// place writes business data and a message in one transaction, which is
// what every caller of this package looks like.
func place(t *testing.T, pool *pgxpool.Pool, orderID string, fail bool) error {
	t.Helper()
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS orders (id text PRIMARY KEY)`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO orders (id) VALUES ($1)`, orderID); err != nil {
		return err
	}
	if _, err := outbox.Write(ctx, tx, "orders.placed", orderID,
		map[string]any{"order_id": orderID}); err != nil {
		return err
	}

	if fail {
		// The business transaction fails after the message was written.
		return errors.New("deliberate failure before commit")
	}
	return tx.Commit(ctx)
}

func TestTheMessageAndTheDataCommitTogether(t *testing.T) {
	pool := fresh(t)
	ctx := context.Background()

	if err := place(t, pool, "order-1", false); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("%d messages in the outbox after one order", n)
	}
}

// TestARolledBackOrderLeavesNoMessage is the half that a publish after
// commit gets wrong in the other direction: the warehouse is told about an
// order that does not exist.
func TestARolledBackOrderLeavesNoMessage(t *testing.T) {
	pool := fresh(t)
	ctx := context.Background()

	if err := place(t, pool, "order-2", true); err == nil {
		t.Fatal("the deliberate failure did not fail")
	}

	var orders, messages int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM orders WHERE id = 'order-2'`).Scan(&orders)
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	if orders != 0 {
		t.Fatal("the order survived a rolled back transaction")
	}
	if messages != 0 {
		t.Fatalf("%d messages left behind by a rolled back order, so somebody would be told about an order that does not exist", messages)
	}
}

func TestRelayPublishes(t *testing.T) {
	pool := fresh(t)
	pub := &recorder{}
	r := relay.New(pool, pub, quiet())
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := place(t, pool, fmt.Sprintf("o%d", i), false); err != nil {
			t.Fatal(err)
		}
	}

	n, err := r.Once(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("the relay handled %d messages, want 5", n)
	}
	if got := len(pub.seen()); got != 5 {
		t.Fatalf("%d messages published", got)
	}

	// A second pass has nothing to do.
	if n, err := r.Once(ctx); err != nil || n != 0 {
		t.Fatalf("a second pass handled %d (err %v), so messages are being republished", n, err)
	}
}

// TestAFailedPublishIsRetriedWithBackoff, and the reason is on the row
// rather than in a log nobody is reading.
func TestAFailedPublishIsRetriedWithBackoff(t *testing.T) {
	pool := fresh(t)
	pub := &recorder{}
	pub.fail.Store(true)
	r := relay.New(pool, pub, quiet())
	ctx := context.Background()

	if err := place(t, pool, "o1", false); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Once(ctx); err != nil {
		t.Fatal(err)
	}

	var attempts int
	var lastErr string
	var nextTry time.Time
	err := pool.QueryRow(ctx,
		`SELECT attempts, coalesce(last_error,''), next_try_at FROM outbox`).Scan(&attempts, &lastErr, &nextTry)
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("attempts is %d after one failure", attempts)
	}
	if lastErr == "" {
		t.Fatal("the row does not say why it failed, so nobody can tell without a debugger")
	}
	if !nextTry.After(time.Now()) {
		t.Fatal("the message is due again immediately, so a broker outage becomes a tight loop")
	}

	// It is not due yet, so the relay passes over it.
	if n, _ := r.Once(ctx); n != 0 {
		t.Fatal("a message in backoff was retried immediately")
	}

	// Once it is due and the broker is back, it goes.
	if _, err := pool.Exec(ctx, `UPDATE outbox SET next_try_at = now()`); err != nil {
		t.Fatal(err)
	}
	pub.fail.Store(false)
	if n, err := r.Once(ctx); err != nil || n != 1 {
		t.Fatalf("handled %d (err %v)", n, err)
	}
	if len(pub.seen()) != 1 {
		t.Fatal("the message was never published")
	}
}

// TestTwoRelaysDoNotPublishTheSameMessage is what SKIP LOCKED is for.
// Without it the second relay queues behind the first and adds nothing;
// with a plain FOR UPDATE and no skip it would block, and with neither it
// would publish duplicates.
func TestTwoRelaysDoNotPublishTheSameMessage(t *testing.T) {
	pool := fresh(t)
	ctx := context.Background()

	const messages = 200
	for i := 0; i < messages; i++ {
		if err := place(t, pool, fmt.Sprintf("o%03d", i), false); err != nil {
			t.Fatal(err)
		}
	}

	pub := &recorder{}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := relay.New(pool, pub, quiet())
			r.Batch = 20
			for {
				n, err := r.Once(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	seen := pub.seen()
	if len(seen) != messages {
		t.Fatalf("%d messages published, want %d", len(seen), messages)
	}
	counts := map[string]int{}
	for _, s := range seen {
		counts[s]++
	}
	for s, c := range counts {
		if c != 1 {
			t.Fatalf("%s was published %d times by four concurrent relays", s, c)
		}
	}

	var unpublished int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&unpublished); err != nil {
		t.Fatal(err)
	}
	if unpublished != 0 {
		t.Fatalf("%d messages left unpublished", unpublished)
	}
}

// TestACrashAfterPublishingRepublishes states the honest cost of this
// design out loud. The relay publishes, then dies before committing, so
// the row rolls back to unpublished and the message goes again. At least
// once, which is why every consumer has to be idempotent.
func TestACrashAfterPublishingRepublishes(t *testing.T) {
	pool := fresh(t)
	ctx := context.Background()

	if err := place(t, pool, "o1", false); err != nil {
		t.Fatal(err)
	}

	// Do what Once does, and roll back instead of committing.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var id int64
	err = tx.QueryRow(ctx, `SELECT id FROM outbox WHERE published_at IS NULL
	                         ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	// The publish happened here. Then the process died.
	_ = tx.Rollback(ctx)

	pub := &recorder{}
	r := relay.New(pool, pub, quiet())
	if n, err := r.Once(ctx); err != nil || n != 1 {
		t.Fatalf("after a crash the message was not picked up again: %d, %v", n, err)
	}
	if len(pub.seen()) != 1 {
		t.Fatal("the message was lost by a crash, which is the thing this design exists to prevent")
	}
}

// TestASlowBrokerDoesNotLoseMessages. The transaction is held across the
// publish, so a timeout leaves the row claimed and then released, not
// marked published.
func TestASlowBrokerLeavesTheMessagePending(t *testing.T) {
	pool := fresh(t)
	pub := &recorder{delay: 50 * time.Millisecond}
	pub.fail.Store(true)
	r := relay.New(pool, pub, quiet())
	ctx := context.Background()

	if err := place(t, pool, "o1", false); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Once(ctx); err != nil {
		t.Fatal(err)
	}

	var published *time.Time
	if err := pool.QueryRow(ctx, `SELECT published_at FROM outbox`).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if published != nil {
		t.Fatal("a message the broker rejected was marked published")
	}
}

func TestAMessageStopsBeingRetriedAndIsNotLost(t *testing.T) {
	pool := fresh(t)
	ctx := context.Background()

	if err := place(t, pool, "o1", false); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE outbox SET attempts = $1, last_error = 'broker is down'`, outbox.MaxAttempts); err != nil {
		t.Fatal(err)
	}

	pub := &recorder{}
	r := relay.New(pool, pub, quiet())
	if n, _ := r.Once(ctx); n != 0 {
		t.Fatal("a message past the attempt limit was retried automatically")
	}

	stuck, err := r.Stuck(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(stuck) != 1 {
		t.Fatalf("%d stuck messages listed, want 1", len(stuck))
	}
	// It is still in the table. A message in the outbox is a fact about
	// something that happened, so deleting it loses data nothing else has.
	if stuck[0].LastError == "" {
		t.Fatal("the stuck message does not say why")
	}

	if err := r.Retry(ctx, stuck[0].ID); err != nil {
		t.Fatal(err)
	}
	if n, err := r.Once(ctx); err != nil || n != 1 {
		t.Fatalf("after a retry the message was not picked up: %d, %v", n, err)
	}
}

func TestPending(t *testing.T) {
	pool := fresh(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := place(t, pool, fmt.Sprintf("o%d", i), false); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx,
		`UPDATE outbox SET created_at = now() - interval '5 minutes' WHERE id = (SELECT min(id) FROM outbox)`); err != nil {
		t.Fatal(err)
	}

	pub := &recorder{}
	r := relay.New(pool, pub, quiet())
	count, oldest, err := r.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("pending is %d, want 3", count)
	}
	if oldest < 4*time.Minute {
		t.Fatalf("the oldest message reads as %s old, want about five minutes", oldest)
	}

	if _, err := r.Once(ctx); err != nil {
		t.Fatal(err)
	}
	count, _, _ = r.Pending(ctx)
	if count != 0 {
		t.Fatalf("%d still pending after a successful pass", count)
	}
}

func TestSweep(t *testing.T) {
	pool := fresh(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := place(t, pool, fmt.Sprintf("o%d", i), false); err != nil {
			t.Fatal(err)
		}
	}
	pub := &recorder{}
	r := relay.New(pool, pub, quiet())
	if _, err := r.Once(ctx); err != nil {
		t.Fatal(err)
	}
	if err := place(t, pool, "still-pending", false); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE outbox SET published_at = now() - interval '40 days' WHERE published_at IS NOT NULL`); err != nil {
		t.Fatal(err)
	}

	n, err := r.Sweep(ctx, 30*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("swept %d rows, want 3", n)
	}
	var left int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 1 {
		t.Fatalf("%d rows left, want the one unpublished message", left)
	}
}

func TestBackoffIsCapped(t *testing.T) {
	var last time.Duration
	for i := 0; i < 20; i++ {
		d := outbox.Backoff(i)
		if d < last {
			t.Fatalf("backoff went backwards at attempt %d: %s after %s", i, d, last)
		}
		if d > 5*time.Minute {
			t.Fatalf("backoff reached %s, over the cap", d)
		}
		last = d
	}
	// A cap matters more than the curve: the thing that fixes a broker
	// outage is somebody restarting it, and the message should go out
	// shortly after they do rather than hours later.
	if outbox.Backoff(19) != 5*time.Minute {
		t.Fatalf("backoff settles at %s, want the five minute cap", outbox.Backoff(19))
	}
}

func TestWriteRefusesAMessageWithNoTopic(t *testing.T) {
	pool := fresh(t)
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	if _, err := outbox.Write(ctx, tx, "", "k", map[string]any{"a": 1}); err == nil {
		t.Fatal("a message with no topic was accepted, and nothing can deliver it")
	}
}

func TestOrderIsPreservedWithinAPartitionKey(t *testing.T) {
	pool := fresh(t)
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := outbox.Write(ctx, tx, "orders.updated", "order-1",
			map[string]any{"seq": i}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	pub := &recorder{}
	r := relay.New(pool, pub, quiet())
	r.Batch = 5
	for {
		n, err := r.Once(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}

	seen := pub.seen()
	if len(seen) != 20 {
		t.Fatalf("%d published, want 20", len(seen))
	}
	for i, s := range seen {
		want := fmt.Sprintf(`"seq": %d`, i)
		if !strings.Contains(s, want) {
			t.Fatalf("message %d is %s, want it to contain %s: order was not preserved", i, s, want)
		}
	}
}
