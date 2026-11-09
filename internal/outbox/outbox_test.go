package outbox_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/usmanvirk/outbox/internal/outbox"
)

// fakeTx records what Write would send, so the parts of Write that are not
// about the database can be tested without one.
type fakeTx struct {
	sql  string
	args []any
	err  error
}

type fakeRow struct {
	id  int64
	err error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) > 0 {
		if p, ok := dest[0].(*int64); ok {
			*p = r.id
		}
	}
	return nil
}

func (f *fakeTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	f.sql, f.args = sql, args
	return fakeRow{id: 99, err: f.err}
}

func TestWriteEncodesThePayloadAndReturnsTheID(t *testing.T) {
	tx := &fakeTx{}
	id, err := outbox.Write(context.Background(), tx, "orders.placed", "order-1",
		map[string]any{"order_id": "order-1", "total": 4999})
	if err != nil {
		t.Fatal(err)
	}
	if id != 99 {
		t.Fatalf("id is %d", id)
	}
	if !strings.Contains(tx.sql, "INSERT INTO outbox") {
		t.Fatalf("the statement is %q", tx.sql)
	}
	if len(tx.args) != 3 {
		t.Fatalf("%d arguments, want topic, key and payload", len(tx.args))
	}
	if tx.args[0] != "orders.placed" || tx.args[1] != "order-1" {
		t.Fatalf("arguments are %v", tx.args[:2])
	}
	raw, ok := tx.args[2].([]byte)
	if !ok {
		t.Fatalf("the payload was passed as %T, not encoded json", tx.args[2])
	}
	if !strings.Contains(string(raw), `"total":4999`) {
		t.Fatalf("the payload is %s", raw)
	}
}

func TestWriteRefusesAMessageNothingCanDeliver(t *testing.T) {
	tx := &fakeTx{}
	if _, err := outbox.Write(context.Background(), tx, "", "k", map[string]any{}); err == nil {
		t.Fatal("a message with no topic was accepted")
	}
}

// TestWriteRefusesAPayloadItCannotEncode. Finding out at the point of the
// write, inside the caller's transaction, means the business change rolls
// back with it. Finding out in the relay means a row nothing can ever
// publish.
func TestWriteRefusesAPayloadItCannotEncode(t *testing.T) {
	tx := &fakeTx{}
	_, err := outbox.Write(context.Background(), tx, "t", "k", make(chan int))
	if err == nil {
		t.Fatal("a payload that cannot be encoded was accepted")
	}
	if !strings.Contains(err.Error(), "payload") {
		t.Fatalf("the error does not say what was wrong: %v", err)
	}
}

func TestWritePassesTheDatabaseErrorUp(t *testing.T) {
	boom := errors.New("constraint violated")
	tx := &fakeTx{err: boom}
	_, err := outbox.Write(context.Background(), tx, "t", "k", map[string]any{})
	if err == nil {
		t.Fatal("a failing insert returned no error")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("the cause was lost: %v", err)
	}
}

func TestBackoff(t *testing.T) {
	first := outbox.Backoff(0)
	if first != 2*time.Second {
		t.Fatalf("the first retry waits %s, want 2s", first)
	}
	if second := outbox.Backoff(1); second != 2*first {
		t.Fatalf("the second waits %s, want %s", second, 2*first)
	}

	var last time.Duration
	for i := 0; i < 30; i++ {
		d := outbox.Backoff(i)
		if d < last {
			t.Fatalf("backoff went backwards at attempt %d", i)
		}
		if d > 5*time.Minute {
			t.Fatalf("backoff reached %s, over the cap", d)
		}
		last = d
	}
	// The ceiling matters more than the curve: what fixes a broker outage
	// is somebody restarting it, and the backlog should drain shortly
	// after they do.
	if outbox.Backoff(29) != 5*time.Minute {
		t.Fatalf("backoff settles at %s, want five minutes", outbox.Backoff(29))
	}
}

func TestSchemaEmbedded(t *testing.T) {
	// A missing embed is a runtime failure on the first start, and this is
	// a one line way to find it at test time instead.
	for _, want := range []string{"CREATE TABLE IF NOT EXISTS outbox", "outbox_pending_idx", "published_at"} {
		if !strings.Contains(outbox.Schema, want) {
			t.Errorf("the embedded schema does not contain %q", want)
		}
	}
}
