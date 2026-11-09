// Package relay publishes what is in the outbox.
package relay

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/usmanvirk/outbox/internal/outbox"
)

// Publisher is the broker.
//
// It must be idempotent from the broker's side or not, it does not matter
// here: this is at least once delivery and the consumer carries that. What
// matters is that Publish either succeeds or returns an error, and that a
// timeout counts as an error whose message may still have been delivered.
type Publisher interface {
	Publish(ctx context.Context, topic, partitionKey string, payload []byte) error
}

type Stats struct {
	Published atomic.Int64
	Failed    atomic.Int64
	Batches   atomic.Int64
	Stuck     atomic.Int64
}

// Relay moves messages from the table to the broker.
type Relay struct {
	Pool      *pgxpool.Pool
	Publisher Publisher
	Log       *slog.Logger

	// Batch is how many messages one pass takes.
	//
	// Small on purpose. A large batch holds its rows locked for as long as
	// the slowest publish in it, and on a crash it is the whole batch that
	// gets republished.
	Batch int

	// Idle is how long to wait when there was nothing to do. The outbox is
	// polled rather than listened to; see the README on LISTEN/NOTIFY.
	Idle time.Duration

	stats Stats
}

func New(pool *pgxpool.Pool, pub Publisher, log *slog.Logger) *Relay {
	return &Relay{Pool: pool, Publisher: pub, Log: log, Batch: 50, Idle: 200 * time.Millisecond}
}

func (r *Relay) Stats() *Stats { return &r.stats }

// Run polls until the context is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		n, err := r.Once(ctx)
		if err != nil {
			r.Log.Error("relay pass failed", "err", err)
		}
		if n == 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(r.Idle):
			}
		}
	}
}

// Once publishes one batch and reports how many messages it handled.
//
// The transaction is held open across the publish, which is the decision
// worth defending. It means a row is locked while its message is in
// flight, so no other relay can take it, and it means a crash rolls the
// row back to unpublished so somebody picks it up. The alternative, taking
// the rows and committing before publishing, loses a message on a crash,
// which is the failure this whole package exists to prevent.
//
// The cost is that a slow broker holds a transaction open. That is bounded
// by the batch size and by the publish timeout, and both are small.
func (r *Relay) Once(ctx context.Context) (int, error) {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// FOR UPDATE SKIP LOCKED is what lets several relays run at once. Each
	// takes rows nobody else holds and passes over the rest instead of
	// queueing behind them, so adding a replica adds throughput rather
	// than contention. Without SKIP LOCKED two relays serialise completely
	// and the second one is decoration.
	rows, err := tx.Query(ctx, `
		SELECT id, topic, partition_key, payload, attempts
		  FROM outbox
		 WHERE published_at IS NULL AND next_try_at <= now() AND attempts < $2
		 ORDER BY id
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`, r.Batch, outbox.MaxAttempts)
	if err != nil {
		return 0, fmt.Errorf("claiming a batch: %w", err)
	}

	var batch []outbox.Message
	for rows.Next() {
		var m outbox.Message
		if err := rows.Scan(&m.ID, &m.Topic, &m.PartitionKey, &m.Payload, &m.Attempts); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scanning: %w", err)
		}
		batch = append(batch, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("claiming a batch: %w", err)
	}
	if len(batch) == 0 {
		return 0, nil
	}
	r.stats.Batches.Add(1)

	published := make([]int64, 0, len(batch))
	for _, m := range batch {
		pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := r.Publisher.Publish(pubCtx, m.Topic, m.PartitionKey, m.Payload)
		cancel()

		if err == nil {
			published = append(published, m.ID)
			r.stats.Published.Add(1)
			continue
		}

		r.stats.Failed.Add(1)
		// Recorded on the row rather than kept in memory, so the backoff
		// survives the relay restarting and so somebody can see why a
		// message is not moving without attaching a debugger.
		next := time.Now().Add(outbox.Backoff(m.Attempts + 1))
		if _, uerr := tx.Exec(ctx, `
			UPDATE outbox
			   SET attempts = attempts + 1, last_error = $2, next_try_at = $3
			 WHERE id = $1`, m.ID, err.Error(), next); uerr != nil {
			return 0, fmt.Errorf("recording the failure of %d: %w", m.ID, uerr)
		}
		if m.Attempts+1 >= outbox.MaxAttempts {
			r.stats.Stuck.Add(1)
			r.Log.Error("message has stopped being retried and needs a person",
				"id", m.ID, "topic", m.Topic, "attempts", m.Attempts+1, "err", err)
		}
	}

	if len(published) > 0 {
		if _, err := tx.Exec(ctx, `
			UPDATE outbox
			   SET published_at = now(), last_error = NULL
			 WHERE id = ANY($1)`, published); err != nil {
			return 0, fmt.Errorf("marking published: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		// Everything in this batch was published and none of it was
		// recorded, so all of it will be published again. At least once,
		// stated plainly.
		return 0, fmt.Errorf("commit after publishing %d messages, which will be republished: %w",
			len(published), err)
	}
	return len(batch), nil
}

// Pending is how many messages are waiting, and how old the oldest is.
//
// Age rather than depth is the number to alarm on, for the same reason as
// in any queue: depth spikes whenever something busy happens and means
// nothing, while age answers the question an operator actually has, which
// is whether the relay is keeping up.
func (r *Relay) Pending(ctx context.Context) (count int64, oldest time.Duration, err error) {
	var oldestAt *time.Time
	err = r.Pool.QueryRow(ctx, `
		SELECT count(*), min(created_at)
		  FROM outbox WHERE published_at IS NULL`).Scan(&count, &oldestAt)
	if err != nil {
		return 0, 0, fmt.Errorf("counting pending: %w", err)
	}
	if oldestAt != nil {
		oldest = time.Since(*oldestAt)
	}
	return count, oldest, nil
}

// Stuck lists messages that have stopped being retried.
func (r *Relay) Stuck(ctx context.Context, limit int) ([]outbox.Message, error) {
	rows, err := r.Pool.Query(ctx, `
		SELECT id, topic, partition_key, payload, attempts, coalesce(last_error, ''), created_at
		  FROM outbox
		 WHERE published_at IS NULL AND attempts >= $1
		 ORDER BY id LIMIT $2`, outbox.MaxAttempts, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []outbox.Message
	for rows.Next() {
		var m outbox.Message
		if err := rows.Scan(&m.ID, &m.Topic, &m.PartitionKey, &m.Payload,
			&m.Attempts, &m.LastError, &m.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// Retry puts a stuck message back in the queue, for after whatever was
// wrong has been fixed.
func (r *Relay) Retry(ctx context.Context, id int64) error {
	tag, err := r.Pool.Exec(ctx, `
		UPDATE outbox SET attempts = 0, next_try_at = now(), last_error = NULL
		 WHERE id = $1 AND published_at IS NULL`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("message %d is not pending", id)
	}
	return nil
}

// Sweep deletes published rows past the retention window.
//
// The outbox is a log of things that happened, and it grows forever
// without this. The window is how long somebody might want to look, not
// how long the message matters, because the message has been delivered.
func (r *Relay) Sweep(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := r.Pool.Exec(ctx,
		`DELETE FROM outbox WHERE published_at IS NOT NULL AND published_at < now() - $1::interval`,
		fmt.Sprintf("%d seconds", int(olderThan.Seconds())))
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ErrNoRows is re-exported so callers do not import pgx for one sentinel.
var ErrNoRows = pgx.ErrNoRows
