# frozen_string_literal: true

require 'benchmark/ips'
require 'pg'
require_relative '../lib/pg_hash_func'
DB_CONFIG = {
  dbname: ENV['PGDATABASE'] || 'postgres',
  user: ENV['PGUSER'] || 'postgres',
  password: ENV['PGPASSWORD'],
  host: ENV['PGHOST'] || 'localhost',
  port: ENV['PGPORT'] || 5432
}.compact

# Constants from the gem
SEED = PgHashFunc::Hasher::HASH_PARTITION_SEED
MAGIC = PgHashFunc::Hasher::PARTITION_MAGIC_CONSTANT
UINT64_MODULUS = PgHashFunc::Hasher::UINT64_MASK + 1 # 2^64

TEST_DATA = [
  [1, 16],
  [-1, 16],
  [540_364, 16],
  [2**31 - 1, 32],
  [-(2**31), 32],
  [2**63 - 1, 64],
  [-(2**63), 64],
  [123_456_789_012_345, 1024],
  [9_223_372_036_854_775_807, 2048]
].freeze

SQL_QUERY = <<~SQL
  SELECT ( ( ((hashint8extended($1::bigint, $2::bigint)::numeric + $3::numeric) % $5::numeric) % $4::numeric ) + $4::numeric ) % $4::numeric;
SQL

begin
  conn = PG.connect(DB_CONFIG)
  puts 'Connected to PostgreSQL.'
rescue PG::ConnectionBad => e
  puts 'Failed to connect to PostgreSQL. Ensure DB is running and configured correctly.'
  puts "Error: #{e.message}"
  exit(1)
end

puts 'Warming up...'

Benchmark.ips do |x|
  x.report('Ruby Calculation') do
    TEST_DATA.each do |key, num_partitions|
      PgHashFunc.calculate_partition_index_bigint(
        value: key,
        num_partitions: num_partitions,
        seed: SEED,
        magic_constant: MAGIC
      )
    end
  end

  x.report('SQL Query') do
    TEST_DATA.each do |key, num_partitions|
      result = conn.exec_params(SQL_QUERY, [key, SEED, MAGIC, num_partitions, UINT64_MODULUS])
      result.getvalue(0, 0).to_i
    end
  end

  x.compare!
end

conn.close if conn && !conn.finished?
puts 'Disconnected from PostgreSQL.'

# Connected to PostgreSQL.
# Warming up...
# ruby 3.4.2 (2025-02-15 revision d2930f8e7a) +PRISM [arm64-darwin24]
# Warming up --------------------------------------
#     Ruby Calculation     6.755k i/100ms
#            SQL Query   320.000 i/100ms
# Calculating -------------------------------------
#     Ruby Calculation     67.103k (± 3.4%) i/s   (14.90 μs/i) -    337.750k in   5.040734s
#            SQL Query      3.192k (± 2.6%) i/s  (313.26 μs/i) -     16.000k in   5.016067s

# Comparison:
#     Ruby Calculation:    67102.7 i/s
#            SQL Query:     3192.2 i/s - 21.02x  slower

# Disconnected from PostgreSQL.
