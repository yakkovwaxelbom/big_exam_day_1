CREATE TABLE intel_signals (
    signal_id VARCHAR(36) NOT NULL,
    timestamp DATETIME(6) NOT NULL,
    entity_id VARCHAR(20) NOT NULL,
    reported_lat DECIMAL(9,6) NOT NULL,
    reported_lon DECIMAL(9,6) NOT NULL,
    signal_type VARCHAR(20),
    priority_level INT NOT NULL,
    distance INT NOT NULL,
    speed INT NOT NULL,

    PRIMARY KEY (signal_id, timestamp, entity_id)
);

CREATE TABLE attacks (
    attack_id VARCHAR(36) NOT NULL,
    timestamp DATETIME(6),
    entity_id VARCHAR(20) NOT NULL,
    weapon_type VARCHAR(100),

    PRIMARY KEY (attack_id)
);

CREATE TABLE damage_reports (
    attack_id VARCHAR(36) NOT NULL,
    timestamp DATETIME(6),
    result VARCHAR(50) NOT NULL,

    PRIMARY KEY (attack_id)
);

CREATE INDEX idx_intel_entity_time
ON intel_signals(entity_id, timestamp);
