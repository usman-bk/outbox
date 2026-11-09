-- The outbox. A message written here is written by the same transaction
-- that changed the business data, so the two cannot disagree.
CREATE TABLE IF NOT EXISTS outbox (
    id          bigserial   PRIMARY KEY,
    topic       text        NOT NULL,
    -- The key a broker partitions on. Messages sharing one are delivered
    -- in the order they were written, which is the only ordering this
    -- gives and the only one most things need.
    partition_key text      NOT NULL DEFAULT '',
    payload     jsonb       NOT NULL,

    created_at  timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    attempts    int         NOT NULL DEFAULT 0,
    last_error  text,
    -- When the relay may next try. Set into the future on a failure, which
    -- is how the backoff is expressed without holding anything in memory.
    next_try_at timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT published_rows_are_settled CHECK (
        published_at IS NULL OR last_error IS NULL
    )
);

-- The relay's only query: unpublished rows that are due, oldest first.
-- Partial, because published rows become the overwhelming majority of the
-- table within a day and the relay never looks at them.
CREATE INDEX IF NOT EXISTS outbox_pending_idx
    ON outbox (next_try_at, id) WHERE published_at IS NULL;

-- Supports the sweep that deletes published rows on age.
CREATE INDEX IF NOT EXISTS outbox_published_at_idx
    ON outbox (published_at) WHERE published_at IS NOT NULL;
