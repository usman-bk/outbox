// Package outbox makes a database write and a published message agree.
//
// The problem, which has no solution that does not look like this:
//
//	tx.Commit()          // the order is placed
//	broker.Publish(...)  // and the warehouse is told
//
// Between those two lines the process can die. The order exists and nothing
// was told about it, and no amount of retrying fixes it because the retry
// is in the process that died. Swapping the order is worse: publish first
// and the commit can fail, so the warehouse ships something nobody bought.
//
// There is no way to make a database transaction and a network call atomic.
// What there is: write the message into the same database, in the same
// transaction, so the two commit together or neither does. A separate
// process then reads the table and publishes.
//
// The cost is honest and worth saying out loud. Delivery becomes at least
// once rather than exactly once, because the relay can publish and then die
// before recording that it did. Every consumer has to be idempotent. That
// is not a flaw in this design, it is the thing that was always true and
// was previously hidden.
package outbox

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

//go:embed schema.sql
var Schema string

// Message is one thing to tell the world about.
type Message struct {
	ID           int64
	Topic        string
	PartitionKey string
	Payload      []byte

	CreatedAt   time.Time
	PublishedAt *time.Time
	Attempts    int
	LastError   string
	NextTryAt   time.Time
}

// Tx is the subset of a transaction this needs, so a caller can pass
// whatever it already has open rather than this package starting its own.
//
// That is the whole point: if this package began its own transaction, the
// message would commit separately from the business data and the design
// would be pointless.
//
// One method, because one is all Write uses. An interface that also
// declared Exec would have to spell its return type as pgconn.CommandTag
// to match, and would then be an interface only pgx can satisfy, which is
// no better than taking pgx.Tx directly and harder to read.
type Tx interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// The assertion that matters. Without it this interface can drift from
// what pgx actually provides and nothing notices until a caller tries to
// pass a real transaction.
var _ Tx = (pgx.Tx)(nil)

// Write puts a message in the outbox, using the caller's transaction.
//
// It takes a Tx and not a pool, and that is not a convenience. A pool would
// mean a second connection and a second transaction, the message would
// commit independently of the business data, and the whole mechanism would
// be a slower version of publishing directly.
func Write(ctx context.Context, tx Tx, topic, partitionKey string, payload any) (int64, error) {
	if topic == "" {
		return 0, fmt.Errorf("message has no topic")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("encoding the payload for %s: %w", topic, err)
	}

	var id int64
	err = tx.QueryRow(ctx,
		`INSERT INTO outbox (topic, partition_key, payload)
		 VALUES ($1, $2, $3) RETURNING id`,
		topic, partitionKey, raw).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("writing to the outbox: %w", err)
	}
	return id, nil
}

// Backoff is how long to wait before trying a message again.
//
// Exponential with a ceiling. The ceiling matters more than the curve: a
// message failing because a broker is down should be retried every few
// minutes forever rather than drifting out to hours, because the thing
// that fixes it is somebody restarting the broker and the message should
// go out shortly after they do.
func Backoff(attempts int) time.Duration {
	const (
		base = 2 * time.Second
		max  = 5 * time.Minute
	)
	d := base
	for i := 0; i < attempts && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	return d
}

// MaxAttempts is when a message stops being retried automatically.
//
// It is not deleted and not dead lettered. A message in the outbox is a
// fact about something that happened in the database, so throwing it away
// loses data that cannot be recovered from anywhere else. It stops being
// retried and starts being somebody's problem, which is what the stuck
// query is for.
const MaxAttempts = 12
