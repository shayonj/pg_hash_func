# frozen_string_literal: true

require "benchmark/ips"
require "pg"
require_relative "../lib/pg_hash_func"
DB_CONFIG = {
  dbname: ENV["PGDATABASE"] || "postgres",
  user: ENV["PGUSER"] || "postgres",
  password: ENV.fetch("PGPASSWORD", nil),
  host: ENV["PGHOST"] || "localhost",
  port: ENV["PGPORT"] || 5432
}.compact

# Constants from the gem
SEED = PgHashFunc::Hasher::HASH_PARTITION_SEED
MAGIC = PgHashFunc::Hasher::PARTITION_MAGIC_CONSTANT
UINT64_MODULUS = PgHashFunc::Hasher::UINT64_MASK + 1 # 2^64

TEST_DATA_BIGINT = [
  [1, 16],
  [-1, 16],
  [540_364, 16],
  [(2**31) - 1, 32],
  [-(2**31), 32],
  [(2**63) - 1, 64],
  [-(2**63), 64],
  [123_456_789_012_345, 1024],
  [9_223_372_036_854_775_807, 2048]
].freeze

TEST_DATA_INT4 = [
  [1, 16],
  [-1, 16],
  [123_456, 16],
  [(2**31) - 1, 32],
  [-(2**31), 32]
].freeze

SQL_QUERY_BIGINT = <<~SQL
  SELECT ( ( ((hashint8extended($1::bigint, $2::bigint)::numeric + $3::numeric) % $5::numeric) % $4::numeric ) + $4::numeric ) % $4::numeric;
SQL

SQL_QUERY_INT4 = <<~SQL
  SELECT (( ( (hashint4extended($1::integer, $2::bigint)::numeric % $4::numeric ) % $3::numeric ) + $3::numeric ) % $3::numeric);
SQL

begin
  conn = PG.connect(DB_CONFIG)
  puts "Connected to PostgreSQL."
rescue PG::ConnectionBad => e
  puts "Failed to connect to PostgreSQL. Ensure DB is running and configured correctly."
  puts "Error: #{e.message}"
  exit(1)
end

puts "Warming up..."

Benchmark.ips do |x|
  x.report("Ruby Calculation (bigint)") do
    TEST_DATA_BIGINT.each do |key, num_partitions|
      PgHashFunc.calculate_partition_index_bigint(
        value: key,
        num_partitions: num_partitions,
        seed: SEED,
        magic_constant: MAGIC
      )
    end
  end

  x.report("SQL Query (bigint)") do
    TEST_DATA_BIGINT.each do |key, num_partitions|
      result = conn.exec_params(SQL_QUERY_BIGINT, [key, SEED, MAGIC, num_partitions, UINT64_MODULUS])
      result.getvalue(0, 0).to_i
    end
  end

  x.report("Ruby Calculation (int4)") do
    TEST_DATA_INT4.each do |key, num_partitions|
      PgHashFunc.calculate_partition_index_int4(
        value: key,
        num_partitions: num_partitions,
        seed: SEED
      )
    end
  end

  x.report("SQL Query (int4)") do
    TEST_DATA_INT4.each do |key, num_partitions|
      result = conn.exec_params(SQL_QUERY_INT4, [key, SEED, num_partitions, UINT64_MODULUS])
      result.getvalue(0, 0).to_i
    end
  end

  x.compare!
end

conn.close if conn && !conn.finished?
puts "Disconnected from PostgreSQL."

# Connected to PostgreSQL.
# Warming up...
# ruby 3.4.2 (2025-02-15 revision d2930f8e7a) +PRISM [arm64-darwin24]
# Warming up --------------------------------------
# Ruby Calculation (bigint)
#                          6.173k i/100ms
#   SQL Query (bigint)   314.000 i/100ms
# Ruby Calculation (int4)
#                         12.411k i/100ms
#     SQL Query (int4)   579.000 i/100ms
# Calculating -------------------------------------
# Ruby Calculation (bigint)
#                          61.105k (± 0.9%) i/s   (16.37 μs/i) -    308.650k in   5.051550s
#   SQL Query (bigint)      3.133k (± 4.3%) i/s  (319.13 μs/i) -     15.700k in   5.021826s
# Ruby Calculation (int4)
#                         121.965k (± 1.0%) i/s    (8.20 μs/i) -    620.550k in   5.088456s
#     SQL Query (int4)      5.949k (± 2.9%) i/s  (168.10 μs/i) -     30.108k in   5.066292s

# Comparison:
# Ruby Calculation (int4):   121964.6 i/s
# Ruby Calculation (bigint):    61105.3 i/s - 2.00x  slower
#     SQL Query (int4):     5948.9 i/s - 20.50x  slower
#   SQL Query (bigint):     3133.5 i/s - 38.92x  slower


# Disconnected from PostgreSQL.
