CREATE DATABASE IF NOT EXISTS meta;
CREATE DATABASE IF NOT EXISTS ops;
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS meta.schema_migrations
(
  version String,
  applied_at DateTime DEFAULT now(),
  checksum String,
  success UInt8,
  notes String
)
ENGINE = MergeTree
ORDER BY (version, applied_at);
