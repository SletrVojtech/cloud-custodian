
CREATE TABLE Entities (
    Id SERIAL PRIMARY KEY,             
    ExternalId VARCHAR(255) UNIQUE,    
    ResourceName VARCHAR(255),
    ResourceType VARCHAR(50),                   -- 'account', 'group', 'resource'
    ParentId INTEGER REFERENCES Entities(Id),
    ProviderName VARCHAR(20),          -- AWS, Azure, K8s
    MetaHash INT,
    Tags JSONB,
    Extras JSONB,
);


CREATE TABLE Costs (
    Id BIGSERIAL,
    EntityId INTEGER REFERENCES Entities(Id),
    BilledCost DOUBLE PRECISION,
    BillingCurrency VARCHAR(3) DEFAULT 'EUR',
    ChargePeriodStart TIMESTAMP WITH TIME ZONE,
    ChargePeriodEnd TIMESTAMP WITH TIME ZONE,
    ServiceCategory VARCHAR(255),
    ServiceName VARCHAR(255),
    SkuPriceId VARCHAR(255),
    
    -- for fast UPSERTS, some data needs to be pre-aggregated.
    PRIMARY KEY(EntityId, ChargePeriodStart, ServiceName, SkuPriceId)
)WITH(
    tsdb.hypertable,
    tsdb.segmentby = 'EntityId',
);

CREATE TABLE Budgets (
    Id SERIAL PRIMARY KEY,
    EntityId INTEGER REFERENCES Entities(Id),
    LimitAmount DECIMAL(18, 4),
    BillingPeriodStart DATE,
    BillingPeriodEnd DATE
);


CREATE TABLE Metrics (
    Id BIGSERIAL,
    Timestamp TIMESTAMP WITH TIME ZONE,
    EntityId INTEGER REFERENCES Entities(Id),
    IntervalMinutes INTEGER,
    MetricType VARCHAR,
    Value DOUBLE PRECISION

    PRIMARY KEY (EntityId, MetricType, Timestamp)
)WITH(
    tsdb.hypertable,
    tsdb.segmentby = ̈́'EntityId',
);

CREATE MATERIALIZED VIEW metrics_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', Timestamp) AS Bucket,
    EntityId,
    MetricType,
    AVG(Value) AS avg_value,
    MAX(Value) AS max_value,
    MIN(Value) AS min_value,
    SUM(Value) AS sum_value,
    COUNT(Value) AS count_value
FROM Metrics
GROUP BY Bucket, EntityId, MetricType;

CREATE MATERIALIZED VIEW metrics_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', Bucket) AS Bucket,
    EntityId,
    MetricType,
    AVG(avg_value) AS avg_value,
    MAX(max_value) AS max_value,
    MIN(min_value) AS min_value,
    SUM(sum_value) AS sum_value,
    SUM(count_value) AS count_value -- sum of aggregated counts
FROM metrics_hourly
GROUP BY Bucket, EntityId, MetricType;
