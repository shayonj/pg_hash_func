# frozen_string_literal: true

require "pg_hash_func"
require "pg" # For database connection

# Shared DB config outside the describe block
DB_CONFIG = {
  dbname: ENV["PGDATABASE"] || "postgres",
  user: ENV["PGUSER"] || "postgres",
  password: ENV.fetch("PGPASSWORD", nil),
  host: ENV["PGHOST"] || "localhost",
  port: ENV["PGPORT"] || 5432
}.compact

RSpec.describe PgHashFunc do
  let(:conn) do
    PG.connect(DB_CONFIG)
  rescue PG::ConnectionBad => e
    raise "Failed to connect to PostgreSQL. Ensure DB is running and configured correctly.\nError: #{e.message}"
  end
  # --- Test Cases ---
  let(:seed) { PgHashFunc::Hasher::HASH_PARTITION_SEED }
  let(:magic) { PgHashFunc::Hasher::PARTITION_MAGIC_CONSTANT }

  after do
    conn.close if conn && !conn.finished?
  end

  # Helper to execute SQL and get the non-negative partition index matching PG internal behavior
  def get_pg_partition_index(value, num_partitions, seed, magic_constant)
    uint64_modulus = 18_446_744_073_709_551_616
    query = <<~SQL
      SELECT ( ( ((hashint8extended($1::bigint, $2::bigint)::numeric + $3::numeric) % $5::numeric) % $4::numeric ) + $4::numeric ) % $4::numeric;
    SQL
    result = conn.exec_params(query, [value, seed, magic_constant, num_partitions, uint64_modulus])
    result.getvalue(0, 0).to_i
  end

  # Helper to execute SQL and get the non-negative partition index for int4
  def get_pg_partition_index_int4(value, num_partitions, seed, magic_constant)
    uint64_modulus = 18_446_744_073_709_551_616
    query = <<~SQL
      SELECT ( ( ((hashint4extended($1::integer, $2::bigint)::numeric + $3::numeric) % $5::numeric) % $4::numeric ) + $4::numeric ) % $4::numeric;
    SQL
    result = conn.exec_params(query, [value, seed, magic_constant, num_partitions, uint64_modulus])
    result.getvalue(0, 0).to_i
  end

  # Structure: [ levels_array ]
  # levels_array: [ [key1, num1], [key2, num2], ... ]
  test_levels_data = [
    [[1, 16]],
    [[0, 16]],
    [[-1, 16]],
    [[540_364, 16]],
    [[540_365, 16]],
    [[(2**31) - 1, 32]],
    [[-(2**31), 32]],
    [[(2**63) - 1, 64]],
    [[-(2**63), 64]],
    [[123_456_789_012_345, 1024]],
    [[9_223_372_036_854_775_807, 2048]],
    [[1, 16], [100, 8]],
    [[0, 16], [0, 8]],
    [[-1, 16], [-10, 8]],
    [[540_364, 16], [12_345, 8]],
    [[540_365, 16], [98_765, 8]],
    [[(2**31) - 1, 32], [-(2**31), 16]],
    [[-(2**63), 64], [(2**63) - 1, 32]],
    [[540_364, 16], [123_456_789_012_345, 1024]],
    [[9_223_372_036_854_775_807, 2048], [123_123_123_123_123_123, 4096]]
  ]

  # --- int4 Test Data ---
  test_levels_data_int4 = [
    [[1, 16]],
    [[0, 16]],
    [[-1, 16]],
    [[123_456, 16]],
    [[2_147_483_647, 32]], # Max int4
    [[-2_147_483_648, 32]], # Min int4
    [[1, 16], [100, 8]],
    [[0, 16], [0, 8]],
    [[-1, 16], [-10, 8]],
    [[123_456, 16], [98_765, 8]],
    [[2_147_483_647, 32], [-2_147_483_648, 16]]
  ]

  # --- bigint Tests ---
  test_levels_data.each do |levels|
    levels.each_with_index do |(key, num_partitions), level_index|
      context "with key=#{key}, num_partitions=#{num_partitions} (Level #{level_index + 1})" do
        it "matches PostgreSQL's partition index calculation (bigint)" do
          expected_index = get_pg_partition_index(key, num_partitions, seed, magic)
          ruby_index = described_class.calculate_partition_index_bigint(
            value: key, num_partitions: num_partitions,
            seed: seed, magic_constant: magic
          )
          expect(ruby_index).to eq(expected_index)
        end
      end
    end
  end

  # --- int4 Tests ---
  test_levels_data_int4.each do |levels|
    levels.each_with_index do |(key, num_partitions), level_index|
      context "with int4 key=#{key}, num_partitions=#{num_partitions} (Level #{level_index + 1})" do
        it "matches PostgreSQL's partition index calculation (int4)" do
          expected_index = get_pg_partition_index_int4(key, num_partitions, seed, magic)
          ruby_index = described_class.calculate_partition_index_int4(value: key, num_partitions: num_partitions,
                                                                      seed: seed, magic_constant: magic)
          expect(ruby_index).to eq(expected_index)
        end
      end
    end
  end

  # Test edge case: 1 partition
  context "with num_partitions = 1" do
    it "returns index 0 for bigint" do
      expect(described_class.calculate_partition_index_bigint(value: 12_345, num_partitions: 1, seed: seed,
                                                              magic_constant: magic)).to eq(0)
      expect(described_class.calculate_partition_index_bigint(value: 0, num_partitions: 1, seed: seed,
                                                              magic_constant: magic)).to eq(0)
      expect(described_class.calculate_partition_index_bigint(value: -99, num_partitions: 1, seed: seed,
                                                              magic_constant: magic)).to eq(0)
    end

    it "returns index 0 for int4" do
      expect(described_class.calculate_partition_index_int4(value: 12_345, num_partitions: 1, seed: seed,
                                                            magic_constant: magic)).to eq(0)
      expect(described_class.calculate_partition_index_int4(value: 0, num_partitions: 1, seed: seed,
                                                            magic_constant: magic)).to eq(0)
      expect(described_class.calculate_partition_index_int4(value: -99, num_partitions: 1, seed: seed,
                                                            magic_constant: magic)).to eq(0)
    end
  end

  # Test invalid partition count
  context "with num_partitions <= 0" do
    it "raises ArgumentError for bigint with 0 partitions" do
      expect do
        described_class.calculate_partition_index_bigint(value: 1, seed: seed, magic_constant: magic, num_partitions: 0)
      end.to raise_error(ArgumentError, /Number of partitions must be positive/)
    end

    it "raises ArgumentError for bigint with -1 partitions" do
      expect do
        described_class.calculate_partition_index_bigint(value: 1, seed: seed, magic_constant: magic,
                                                         num_partitions: -1)
      end.to raise_error(ArgumentError, /Number of partitions must be positive/)
    end

    it "raises ArgumentError for int4 with 0 partitions" do
      expect do
        described_class.calculate_partition_index_int4(value: 1, seed: seed, magic_constant: magic, num_partitions: 0)
      end.to raise_error(ArgumentError, /Number of partitions must be positive/)
    end

    it "raises ArgumentError for int4 with -1 partitions" do
      expect do
        described_class.calculate_partition_index_int4(value: 1, seed: seed, magic_constant: magic, num_partitions: -1)
      end.to raise_error(ArgumentError, /Number of partitions must be positive/)
    end
  end

  # Can optionally add tests specifically for hashint8extended if desired
  # context "hashint8extended" do
  #   it "calculates hash values" do
  #     # ... tests comparing raw hash output ...
  #   end
  # end
end
