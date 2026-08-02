-- Window decision audit table for RL research (Phase 1+)
CREATE TABLE IF NOT EXISTS window_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    tenant VARCHAR(128) NOT NULL,
    pipeline VARCHAR(128) NOT NULL,
    queue_key VARCHAR(512) NOT NULL,
    window_size INT NOT NULL,
    queue_depth INT NOT NULL,
    failure_rate DOUBLE NOT NULL,
    hour_sin DOUBLE NOT NULL,
    hour_cos DOUBLE NOT NULL,
    executor_util DOUBLE NOT NULL,
    action_delta INT NULL,
    source VARCHAR(64) NOT NULL,
    cycle_succeeded TINYINT NULL,
    reward DOUBLE NULL,
    mode VARCHAR(32) NOT NULL DEFAULT 'shadow'
);

CREATE INDEX IF NOT EXISTS idx_window_events_recorded_at
    ON window_events (recorded_at);
