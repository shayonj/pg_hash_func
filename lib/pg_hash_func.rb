# frozen_string_literal: true

require_relative "pg_hash_func/version"
require_relative "pg_hash_func/hasher"

# Module providing functions to replicate PostgreSQL's bigint hash partitioning logic.
module PgHashFunc
  class Error < StandardError; end

  # Provides functions to replicate PostgreSQL's bigint hash partitioning logic.

  # Expose the raw hash function (`hashint8extended`) if needed.
  # This is the core PostgreSQL hash function for bigint values.
  #
  # @param value [Integer] The integer value to hash.
  # @param seed [Integer] The 64-bit seed. Defaults to PostgreSQL's standard HASH_PARTITION_SEED.
  # @return [Integer] The 64-bit hash result (as a Ruby Integer).
  def self.hashint8extended(value:, seed: Hasher::HASH_PARTITION_SEED)
    Hasher.hashint8extended(value: value, seed: seed)
  end

  # Calculates the target partition index for a given bigint (int8) value based on
  # PostgreSQL's default hash partitioning strategy.
  # Mimics (hashint8extended(value, seed) + magic) % num_partitions using uint64 arithmetic.
  #
  # @param value [Integer] The partitioning key value (treated as bigint).
  # @param num_partitions [Integer] The number of partitions for this level.
  # @param seed [Integer] The 64-bit seed. Defaults to PostgreSQL's standard HASH_PARTITION_SEED.
  # @param magic_constant [Integer] The magic constant. Defaults to PostgreSQL's standard PARTITION_MAGIC_CONSTANT.
  # @return [Integer] The calculated partition index (0-based).
  def self.calculate_partition_index_bigint(value:, num_partitions:, seed: Hasher::HASH_PARTITION_SEED,
                                     magic_constant: Hasher::PARTITION_MAGIC_CONSTANT)
    Hasher.calculate_partition_index_bigint(value: value, seed: seed, magic_constant: magic_constant,
                                     num_partitions: num_partitions)
  end

  # Calculates the target partition index for a given integer (int4) or smallint (int2) value based on
  # PostgreSQL's default hash partitioning strategy.
  # Mimics (hashint4extended(value, seed) + magic) % num_partitions using uint64 arithmetic.
  # Note: PostgreSQL uses the same hash function (`hashint4extended` equivalent) for both int2 and int4.
  #
  # @param value [Integer] The partitioning key value (treated as int4/int2).
  # @param num_partitions [Integer] The number of partitions for this level.
  # @param seed [Integer] The 64-bit seed. Defaults to PostgreSQL's standard HASH_PARTITION_SEED.
  # @param magic_constant [Integer] The magic constant. Defaults to PostgreSQL's standard PARTITION_MAGIC_CONSTANT.
  # @return [Integer] The calculated partition index (0-based).
  def self.calculate_partition_index_int4(value:, num_partitions:, seed: Hasher::HASH_PARTITION_SEED,
                                         magic_constant: Hasher::PARTITION_MAGIC_CONSTANT)
    Hasher.calculate_partition_index_int4(value: value, seed: seed, magic_constant: magic_constant,
                                          num_partitions: num_partitions)
  end
end
