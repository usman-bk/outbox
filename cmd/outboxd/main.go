// Command outboxd runs the relay, and can demonstrate what it is for.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/usmanvirk/outbox/internal/outbox"
	"github.com/usmanvirk/outbox/internal/relay"
)

// logPublisher stands in for the broker. Swapping in RabbitMQ or Kafka
// changes this one type and nothing else, because the relay talks to an
// interface.
type logPublisher struct {
	log  *slog.Logger
	fail bool
}

func (p logPublisher) Publish(ctx context.Context, topic, key string, payload []byte) error {
	if p.fail {
		return fmt.Errorf("broker is down")
	}
	p.log.Info("published", "topic", topic, "key", key, "bytes", len(payload))
	return nil
}

func main() {
	var (
		dsn       = flag.String("database-url", os.Getenv("DATABASE_URL"), "Postgres DSN")
		seed      = flag.Int("seed", 0, "write this many messages first, as a demonstration")
		once      = flag.Bool("once", false, "run a single pass and exit")
		brokerOut = flag.Bool("broker-down", false, "make every publish fail, to watch the backoff")
		retention = flag.Duration("retention", 30*24*time.Hour, "how long published rows are kept")
		sweepFreq = flag.Duration("sweep-every", time.Hour, "how often published rows are deleted")
		alarm     = flag.Duration("age-alarm", 2*time.Minute, "log loudly when the oldest pending message is older than this")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if *dsn == "" {
		log.Error("database-url is required")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		log.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, outbox.Schema); err != nil {
		log.Error("schema", "err", err)
		os.Exit(1)
	}

	if *seed > 0 {
		if err := seedMessages(ctx, pool, *seed); err != nil {
			log.Error("seeding", "err", err)
			os.Exit(1)
		}
		log.Info("seeded", "messages", *seed)
	}

	r := relay.New(pool, logPublisher{log: log, fail: *brokerOut}, log)

	if *once {
		n, err := r.Once(ctx)
		if err != nil {
			log.Error("pass failed", "err", err)
			os.Exit(1)
		}
		count, oldest, _ := r.Pending(ctx)
		log.Info("pass finished", "handled", n, "pending", count, "oldest", oldest.String())
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = r.Run(ctx) }()

	wg.Add(1)
	go func() { defer wg.Done(); watch(ctx, log, r, *alarm, *retention, *sweepFreq) }()

	log.Info("relay running")
	<-ctx.Done()
	log.Info("draining")
	wg.Wait()

	s := r.Stats()
	log.Info("stopped",
		"published", s.Published.Load(), "failed", s.Failed.Load(), "stuck", s.Stuck.Load())
}

// watch is the operational half: how far behind the relay is, and the
// messages that have given up.
func watch(ctx context.Context, log *slog.Logger, r *relay.Relay, alarm, retention, sweepEvery time.Duration) {
	check := time.NewTicker(15 * time.Second)
	defer check.Stop()
	sweep := time.NewTicker(sweepEvery)
	defer sweep.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-check.C:
			count, oldest, err := r.Pending(ctx)
			if err != nil {
				log.Error("counting pending", "err", err)
				continue
			}
			// Age and not depth. Depth spikes whenever something busy
			// happens and means nothing; age answers whether the relay is
			// keeping up.
			if oldest > alarm {
				log.Error("the outbox is falling behind",
					"pending", count, "oldest", oldest.String(), "alarm", alarm.String())
			}
			if stuck, err := r.Stuck(ctx, 5); err == nil && len(stuck) > 0 {
				for _, m := range stuck {
					log.Error("message needs a person",
						"id", m.ID, "topic", m.Topic, "attempts", m.Attempts, "last_error", m.LastError)
				}
			}

		case <-sweep.C:
			n, err := r.Sweep(ctx, retention)
			if err != nil {
				log.Error("sweep", "err", err)
				continue
			}
			if n > 0 {
				log.Info("swept published rows", "rows", n)
			}
		}
	}
}

// seedMessages writes business rows and their messages in one transaction
// each, which is the only way this package is meant to be used.
func seedMessages(ctx context.Context, pool *pgxpool.Pool, n int) error {
	if _, err := pool.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS orders (id text PRIMARY KEY, placed_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		id := fmt.Sprintf("order-%d-%d", time.Now().UnixNano(), i)
		if _, err := tx.Exec(ctx, `INSERT INTO orders (id) VALUES ($1)`, id); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if _, err := outbox.Write(ctx, tx, "orders.placed", id,
			map[string]any{"order_id": id, "seq": i}); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}
