# frozen_string_literal: true

# Based on PostgreSQL's src/common/hashfn.c (Bob Jenkins' lookup3 hash)
# and src/backend/access/hash/hashfunc.c

# Namespace for PgHashFunc implementation details.
module PgHashFunc
  # Internal implementation of PostgreSQL hashing logic.
  module Hasher
    # Constants derived from PostgreSQL source/behavior
    HASH_PARTITION_SEED = 0x7A5B22367996DCFD
    PARTITION_MAGIC_CONSTANT = 0x4992394d24f64163

    UINT32_MASK = 0xFFFFFFFF
    UINT64_MASK = 0xFFFFFFFFFFFFFFFF

    # Corresponds to rot(x, k) -> pg_rotate_left32(x, k)
    def self.rot(value, rotation_bits)
      value &= UINT32_MASK
      (((value << rotation_bits) | (value >> (32 - rotation_bits))) & UINT32_MASK)
    end

    # Corresponds to mix(a, b, c) macro in hashfn.c
    def self.mix(state)
      a, b, c = state
      a = (a - c) & UINT32_MASK
      a ^= rot(c, 4)
      c = (c + b) & UINT32_MASK
      b = (b - a) & UINT32_MASK
      b ^= rot(a, 6)
      a = (a + c) & UINT32_MASK
      c = (c - b) & UINT32_MASK
      c ^= rot(b, 8)
      b = (b + a) & UINT32_MASK
      a = (a - c) & UINT32_MASK
      a ^= rot(c, 16)
      c = (c + b) & UINT32_MASK
      b = (b - a) & UINT32_MASK
      b ^= rot(a, 19)
      a = (a + c) & UINT32_MASK
      c = (c - b) & UINT32_MASK
      c ^= rot(b, 4)
      b = (b + a) & UINT32_MASK
      [a, b, c]
    end

    # Corresponds to final(a, b, c) macro in hashfn.c
    def self.final(state)
      a, b, c = state
      c ^= b
      c = (c - rot(b, 14)) & UINT32_MASK
      a ^= c
      a = (a - rot(c, 11)) & UINT32_MASK
      b ^= a
      b = (b - rot(a, 25)) & UINT32_MASK
      c ^= b
      c = (c - rot(b, 16)) & UINT32_MASK
      a ^= c
      a = (a - rot(c, 4)) & UINT32_MASK
      b ^= a
      b = (b - rot(a, 14)) & UINT32_MASK
      c ^= b
      c = (c - rot(b, 24)) & UINT32_MASK
      [a, b, c]
    end

    # Corresponds to hash_bytes_uint32_extended(uint32 k, uint64 seed)
    # This implementation is based on analysis of specific PostgreSQL code paths
    # related to partitioning, and may differ slightly from a general lookup3 implementation.
    def self.hash_uint32_extended(key_value, seed)
      key_value &= UINT32_MASK
      seed &= UINT64_MASK

      initval = 0x9e3779b9 + 4 + 3_923_095
      a = b = c = initval & UINT32_MASK

      # Perturb state with seed parts and mix if seed is non-zero
      if seed != 0
        a = (a + (seed >> 32)) & UINT32_MASK
        b = (b + (seed & UINT32_MASK)) & UINT32_MASK
        a, b, c = mix([a, b, c])
      end

      a = (a + key_value) & UINT32_MASK
      _, b, c = final([a, b, c])

      (((b.to_i << 32) | c.to_i) & UINT64_MASK)
    end

    # Corresponds to hashint8extended(int64 val, uint64 seed) logic
    def self.hashint8extended(value:, seed:)
      val = value.to_i
      seed &= UINT64_MASK

      val_masked64 = val & UINT64_MASK
      lohalf = (val_masked64 & UINT32_MASK)
      hihalf = ((val_masked64 >> 32) & UINT32_MASK)

      val_int64 = val_masked64 > 0x7FFFFFFFFFFFFFFF ? (val_masked64 - (1 << 64)) : val_masked64
      is_positive_or_zero_int64 = (val_int64 >= 0)

      lohalf ^= if is_positive_or_zero_int64
                  hihalf
                else
                  (~hihalf & UINT32_MASK)
                end

      hash_uint32_extended(lohalf, seed)
    end

    # Corresponds to hashint4extended(int32 val, uint64 seed) logic
    def self.hashint4extended(value:, seed:)
      val32 = value.to_i & UINT32_MASK
      hash_uint32_extended(val32, seed & UINT64_MASK)
    end

    # Calculates the target partition index for a given bigint value.
    def self.calculate_partition_index_bigint(value:, seed:, magic_constant:, num_partitions:)
      raise ArgumentError, "Number of partitions must be positive" unless num_partitions.positive?

      hash_val = hashint8extended(value: value, seed: seed)

      result = (hash_val + magic_constant) & UINT64_MASK
      idx = result % num_partitions
      idx.to_i
    end

    # Calculates the target partition index for a given int4 value.
    def self.calculate_partition_index_int4(value:, seed:, magic_constant:, num_partitions:)
      raise ArgumentError, "Number of partitions must be positive" unless num_partitions.positive?

      hash_val = hashint4extended(value: value, seed: seed)

      result = (hash_val + magic_constant) & UINT64_MASK
      idx = result % num_partitions
      idx.to_i
    end
  end
end
