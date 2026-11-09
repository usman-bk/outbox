# outbox

_Built with AI assistance._

Making a database write and a published message agree.

```go
tx.Commit()          // the order is placed
broker.Publish(...)  // and the warehouse is told
```

Between those two lines the process can die. The order exists and nobody
was told, and retrying does not help because the retry was in the process
that died. Swapping them is worse: publish first and the commit can fail,
so the warehouse ships something nobody bought.

There is no way to make a database transaction and a network call atomic.
What there is: write the message into the same database in the same
transaction, so the two commit together or neither does, and have a
separate process read the table and publish.

Go 1.23, Postgres.

```
internal/outbox/schema.sql   the table, and the index the relay lives on
internal/outbox/outbox.go    Write, which takes your transaction
internal/relay/relay.go      the publisher loop
cmd/outboxd/
```

## Write takes your transaction, not a pool

```go
tx, _ := pool.Begin(ctx)
tx.Exec(ctx, `INSERT INTO orders ...`)
outbox.Write(ctx, tx, "orders.placed", orderID, payload)
tx.Commit(ctx)
```

That signature is the whole design. A pool would open a second connection
and a second transaction, the message would commit independently of the
order, and this would be a slower way of publishing directly.

The interface has one method, because `Write` only needs `QueryRow`. An
interface that also declared `Exec` would have to spell its return type as
`pgconn.CommandTag` to match, which makes it an interface only pgx can
satisfy and no better than taking `pgx.Tx`. There is a compile-time
assertion that `pgx.Tx` still fits, because without one the interface can
drift and nothing notices until a caller tries to pass a real transaction.

## The cost, stated plainly

**Delivery is at least once.** The relay can publish and then die before
recording that it did, and the message goes again. Every consumer has to be
idempotent.

That is not a flaw introduced here. It was always true of `commit` followed
by `publish`; it was just hidden, and hidden in the direction that loses
messages rather than repeats them. `TestACrashAfterPublishingRepublishes`
does exactly that: publishes, rolls back instead of committing, and asserts
the message goes out again.

## The relay holds its transaction across the publish

```sql
SELECT ... FROM outbox
 WHERE published_at IS NULL AND next_try_at <= now() AND attempts < 12
 ORDER BY id LIMIT 50
 FOR UPDATE SKIP LOCKED
```

...publish each one, mark them, commit.

Holding the transaction open across a network call is the decision worth
defending. It means a crash rolls the rows back to unpublished so somebody
picks them up. The alternative, claiming rows and committing before
publishing, loses a message on a crash, which is the thing this package
exists to prevent.

The cost is a transaction open for as long as the slowest publish in the
batch. That is bounded by the batch size, 50, and by a five second publish
timeout. A larger batch holds more rows locked and republishes more of them
after a crash.

**`SKIP LOCKED` is what makes a second replica worth having.** Each relay
takes rows nobody else holds and steps over the rest instead of queueing
behind them. Without it, two relays serialise completely and the second is
decoration. With no locking at all they publish duplicates, which
`TestTwoRelaysDoNotPublishTheSameMessage` demonstrates: four relays, two
hundred messages, and removing the `FOR UPDATE SKIP LOCKED` makes it fail.

## Failures are recorded on the row

```
  id  | attempts |   last_error   | backing_off
------+----------+----------------+-------------
 1467 |        1 | broker is down | t
```

The attempt count, the reason and the next due time live in the table, so
the backoff survives a relay restart and somebody can see why a message is
not moving without attaching a debugger.

The backoff doubles from two seconds and stops at five minutes. The ceiling
matters more than the curve: what fixes a broker outage is a person
restarting the broker, and the messages should go out shortly after they do
rather than hours later because the curve kept growing.

## A message that gives up is not deleted

After twelve attempts the relay stops picking it up. It is not dead
lettered and not dropped.

A row in this table is a fact about something that already happened in the
database. Throwing it away loses information that exists nowhere else, and
a dead letter queue is just a second place to lose it. It stops being
retried and starts being somebody's problem: `Stuck` lists them with their
last error, and `Retry` puts one back once whatever was wrong is fixed.

## Polling, not LISTEN/NOTIFY

The relay polls every 200 milliseconds when idle.

`LISTEN`/`NOTIFY` would remove that latency and is tempting. It also needs
a dedicated connection per listener, drops notifications if the listener is
not connected at that moment, and does not survive a failover, so the poll
has to exist anyway as the thing that catches what the notification missed.
Given that, the notification is an optimisation on a 200 millisecond delay,
and the polling query is an index-only scan over a partial index of
unpublished rows, which is a handful of rows.

Worth adding when the latency matters. Not worth adding first.

## Alarm on age, not depth

`Pending` returns the count and the age of the oldest unpublished message.
Depth spikes whenever something busy happens and means nothing. Age answers
the question an operator actually has, which is whether the relay is
keeping up.

## Running the demo

    createdb outbox
    DATABASE_URL=postgres://localhost/outbox?sslmode=disable \
      go run ./cmd/outboxd -seed 3 -once -broker-down

    # then, with the broker back
    go run ./cmd/outboxd -once

## Tests, and why most of them need a database

    go test ./...                      7 cases without a database
    DATABASE_URL=... go test ./...     19, which is where the substance is

Almost everything here needs a real database, and that is correct rather
than a gap: what is under test is `SKIP LOCKED`, a transaction held across
a network call, and what a rollback does to a claimed row. A mock of
Postgres cannot have any of those properties, so a test against one would
be a test of the mock.

## Known gaps

**No ordering across partition keys.** Messages sharing a key are published
in the order they were written, because the relay reads by id. Two relays
working at once can interleave different keys, which is what the key is for.

**No transactional read of the payload.** The message carries what the
writer put in it. If it instead carried an id and the consumer read the row
back, it would read whatever the row says later, which is a different and
usually worse thing.

**No schema for the payload.** It is `jsonb`, and nothing validates it. A
contract check on the payloads is the obvious next thing and is its own
problem.
