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

  let(:seed) { PgHashFunc::Hasher::HASH_PARTITION_SEED }
  let(:magic) { PgHashFunc::Hasher::PARTITION_MAGIC_CONSTANT }

  after do
    conn.close if conn && !conn.finished?
  end

  def get_pg_partition_index(value, num_partitions, seed, magic_constant)
    uint64_modulus = 18_446_744_073_709_551_616
    query = <<~SQL
      SELECT ( ( ((hashint8extended($1::bigint, $2::bigint)::numeric + $3::numeric) % $5::numeric) % $4::numeric ) + $4::numeric ) % $4::numeric;
    SQL
    result = conn.exec_params(query, [value, seed, magic_constant, num_partitions, uint64_modulus])
    result.getvalue(0, 0).to_i
  end

  def get_pg_partition_index_int4(value, num_partitions, seed, _magic_constant)
    # For int4 (and int2), PostgreSQL does *not* add the partition magic constant
    # when computing the remainder (see get_hash_partition_greatest_modulus_int4).
    uint64_modulus = 18_446_744_073_709_551_616
    query = <<~SQL
      SELECT (( ( (hashint4extended($1::integer, $2::bigint)::numeric % $4::numeric ) % $3::numeric ) + $3::numeric ) % $3::numeric);
    SQL
    result = conn.exec_params(query, [value, seed, num_partitions, uint64_modulus])
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
          ruby_index = described_class.calculate_partition_index_int4(
            value: key, num_partitions: num_partitions,
            seed: seed
          )
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
      expect(described_class.calculate_partition_index_int4(value: 12_345, num_partitions: 1,
                                                            seed: seed)).to eq(0)
      expect(described_class.calculate_partition_index_int4(value: 0, num_partitions: 1,
                                                            seed: seed)).to eq(0)
      expect(described_class.calculate_partition_index_int4(value: -99, num_partitions: 1,
                                                            seed: seed)).to eq(0)
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
        described_class.calculate_partition_index_int4(value: 1, seed: seed, num_partitions: 0)
      end.to raise_error(ArgumentError, /Number of partitions must be positive/)
    end

    it "raises ArgumentError for int4 with -1 partitions" do
      expect do
        described_class.calculate_partition_index_int4(value: 1, seed: seed, num_partitions: -1)
      end.to raise_error(ArgumentError, /Number of partitions must be positive/)
    end
  end

  # Can optionally add tests specifically for hashint8extended if desired
  # context "hashint8extended" do
  #   it "calculates hash values" do
  #     # ... tests comparing raw hash output ...
  #   end
  # end

  describe "End-to-end partitioning integration tests" do
    let(:num_partitions_bigint) { 4 }
    let(:parent_table_bigint) { "test_e2e_events_bigint" }
    let(:partition_key_bigint) { "event_id" }
    let(:num_partitions_int4) { 3 }
    let(:parent_table_int4) { "test_e2e_logs_int4" }
    let(:partition_key_int4) { "log_id" }
    let(:db_conn) { PG.connect(DB_CONFIG) }

    around do |example|
      db_conn.exec("DROP TABLE IF EXISTS #{parent_table_bigint} CASCADE;")
      db_conn.exec("DROP TABLE IF EXISTS #{parent_table_int4} CASCADE;")

      db_conn.exec(<<~SQL)
        CREATE TABLE #{parent_table_bigint} (
          #{partition_key_bigint} BIGINT NOT NULL,
          data TEXT
        ) PARTITION BY HASH (#{partition_key_bigint});
      SQL

      (0...num_partitions_bigint).each do |i|
        db_conn.exec(<<~SQL)
          CREATE TABLE #{parent_table_bigint}_#{i}
          PARTITION OF #{parent_table_bigint}
          FOR VALUES WITH (MODULUS #{num_partitions_bigint}, REMAINDER #{i});
        SQL
      end

      db_conn.exec(<<~SQL)
        CREATE TABLE #{parent_table_int4} (
          #{partition_key_int4} INTEGER NOT NULL,
          message TEXT
        ) PARTITION BY HASH (#{partition_key_int4});
      SQL

      (0...num_partitions_int4).each do |i|
        db_conn.exec(<<~SQL)
          CREATE TABLE #{parent_table_int4}_#{i}
          PARTITION OF #{parent_table_int4}
          FOR VALUES WITH (MODULUS #{num_partitions_int4}, REMAINDER #{i});
        SQL
      end

      example.run

      begin
        db_conn.exec("DROP TABLE IF EXISTS #{parent_table_bigint} CASCADE;")
        db_conn.exec("DROP TABLE IF EXISTS #{parent_table_int4} CASCADE;")
      ensure
        db_conn.close unless db_conn.finished?
      end
    end

    context "when using bigint partitioned table" do
      it "inserts data into the correct partition and can be queried directly" do
        test_id = 123_456_789_012_345
        test_data_val = "Bigint event data for #{test_id}"

        expected_partition_index = described_class.calculate_partition_index_bigint(
          value: test_id,
          num_partitions: num_partitions_bigint
        )
        expected_partition_table_name = "#{parent_table_bigint}_#{expected_partition_index}"

        db_conn.exec_params(
          "INSERT INTO #{parent_table_bigint} (#{partition_key_bigint}, data) VALUES ($1, $2)",
          [test_id, test_data_val]
        )

        result_partition = db_conn.exec_params(
          "SELECT data FROM #{expected_partition_table_name} WHERE #{partition_key_bigint} = $1",
          [test_id]
        )
        expect(result_partition.ntuples).to eq(1)
        expect(result_partition.getvalue(0, 0)).to eq(test_data_val)

        result_parent = db_conn.exec_params(
          "SELECT data FROM #{parent_table_bigint} WHERE #{partition_key_bigint} = $1",
          [test_id]
        )
        expect(result_parent.ntuples).to eq(1)
        expect(result_parent.getvalue(0, 0)).to eq(test_data_val)
      end
    end

    context "when using int4 partitioned table" do
      it "inserts data into the correct partition and can be queried directly" do
        test_id = 987_654
        test_message_val = "Int4 log message for #{test_id}"

        expected_partition_index = described_class.calculate_partition_index_int4(
          value: test_id,
          num_partitions: num_partitions_int4
        )
        expected_partition_table_name = "#{parent_table_int4}_#{expected_partition_index}"

        db_conn.exec_params(
          "INSERT INTO #{parent_table_int4} (#{partition_key_int4}, message) VALUES ($1, $2)",
          [test_id, test_message_val]
        )

        result_partition = db_conn.exec_params(
          "SELECT message FROM #{expected_partition_table_name} WHERE #{partition_key_int4} = $1",
          [test_id]
        )
        expect(result_partition.ntuples).to eq(1)
        expect(result_partition.getvalue(0, 0)).to eq(test_message_val)

        result_parent = db_conn.exec_params(
          "SELECT message FROM #{parent_table_int4} WHERE #{partition_key_int4} = $1",
          [test_id]
        )
        expect(result_parent.ntuples).to eq(1)
        expect(result_parent.getvalue(0, 0)).to eq(test_message_val)
      end
    end
  end

  # ------------------------------------------------------------------------
  # Multi-level (2- and 3-level) hash-partition integration tests
  # ------------------------------------------------------------------------
  describe "Multi-level hash partitioning" do
    let(:big_parent) { "multi_big_parent" }
    let(:int_parent) { "multi_int_parent" }

    around do |example|
      db = PG.connect(DB_CONFIG)

      db.exec("DROP TABLE IF EXISTS #{big_parent} CASCADE;")
      db.exec("DROP TABLE IF EXISTS #{int_parent} CASCADE;")

      db.exec(<<~SQL)
        CREATE TABLE #{big_parent} (event_id bigint) PARTITION BY HASH (event_id);
      SQL

      4.times do |i|
        child = "#{big_parent}_l1_#{i}"
        db.exec("CREATE TABLE #{child} PARTITION OF #{big_parent} FOR VALUES WITH (MODULUS 4, REMAINDER #{i}) " \
                "PARTITION BY HASH (event_id);")
        2.times do |j|
          leaf = "#{child}_l2_#{j}"
          db.exec("CREATE TABLE #{leaf} PARTITION OF #{child} FOR VALUES WITH (MODULUS 2, REMAINDER #{j});")
        end
      end

      db.exec(<<~SQL)
        CREATE TABLE #{int_parent} (log_id int) PARTITION BY HASH (log_id);
      SQL

      3.times do |a|
        l1 = "#{int_parent}_l1_#{a}"
        db.exec("CREATE TABLE #{l1} PARTITION OF #{int_parent} FOR VALUES WITH (MODULUS 3, REMAINDER #{a}) " \
                "PARTITION BY HASH (log_id);")
        3.times do |b|
          l2 = "#{l1}_l2_#{b}"
          db.exec("CREATE TABLE #{l2} PARTITION OF #{l1} FOR VALUES WITH (MODULUS 3, REMAINDER #{b}) " \
                  "PARTITION BY HASH (log_id);")
          2.times do |c|
            leaf = "#{l2}_l3_#{c}"
            db.exec("CREATE TABLE #{leaf} PARTITION OF #{l2} FOR VALUES WITH (MODULUS 2, REMAINDER #{c});")
          end
        end
      end

      example.run

      begin
        db.exec("DROP TABLE IF EXISTS #{big_parent} CASCADE;")
        db.exec("DROP TABLE IF EXISTS #{int_parent} CASCADE;")
      ensure
        db.close unless db.finished?
      end
    end

    it "routes 10 bigint keys correctly across 2 levels" do
      db = PG.connect(DB_CONFIG)
      keys = [1, 42, 123_456_789, -4, 17, 88, 900, 1_000_000, -99, (2**63) - 10]

      keys.each do |k|
        idx_l1 = described_class.calculate_partition_index_bigint(value: k, num_partitions: 4)
        idx_l2 = described_class.calculate_partition_index_bigint(value: k, num_partitions: 2)
        leaf_table = "#{big_parent}_l1_#{idx_l1}_l2_#{idx_l2}"

        db.exec_params("INSERT INTO #{big_parent} (event_id) VALUES ($1)", [k])
        res = db.exec_params("SELECT 1 FROM #{leaf_table} WHERE event_id=$1", [k])
        expect(res.ntuples).to eq(1)
      end
      db.close
    end

    it "routes 10 int4 keys correctly across 3 levels" do
      db = PG.connect(DB_CONFIG)
      keys = [0, -1, 1, 5, 12, 34, 77, 123_456, 2_000_000, -2_147_483_648]

      keys.each do |k|
        described_class.calculate_partition_index_int4(value: k, num_partitions: 3)
        db.exec_params("INSERT INTO #{int_parent} (log_id) VALUES ($1)", [k])
        res = db.exec_params("SELECT tableoid::regclass::text FROM #{int_parent} WHERE log_id=$1", [k])
        expect(res.getvalue(0, 0)).to match(/^#{int_parent}_l1_\d+_l2_\d+_l3_\d+$/)
      end
      db.close
    end
  end
end
