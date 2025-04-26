# pg_hash_func

[![CI](https://github.com/shayonj/pg_hash_func/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/shayonj/pg_hash_func/actions/workflows/ci.yml)
[![Gem Version](https://badge.fury.io/rb/pg_hash_func.svg)](https://badge.fury.io/rb/pg_hash_func)

Determine the target partition index for an integer key according to PostgreSQL's default hash strategy, without querying the database.

This gem replicates the hashing logic PostgreSQL's `hashint8extended` (for `bigint`) and `hashint4extended` (for `integer` and `smallint`) in [src/backend/access/hash/hashfunc.c](https://github.com/postgres/postgres/blob/master/src/backend/access/hash/hashfunc.c)

**Supported Types:**

- **`bigint` (`int8`)**: Use `PgHashFunc.calculate_partition_index_bigint`.
- **`integer` (`int4`)** and **`smallint` (`int2`)**: Use `PgHashFunc.calculate_partition_index_int4`. (PostgreSQL uses the same underlying hash function for both `int4` and `int2`.)

**Limitations:**

- Only replicates the default `hash` partitioning strategy.
- Only supports integer-based keys (`bigint`, `integer`, `smallint`).
- Does not support hashing other data types (text, dates, floats, etc.).
- Does not support other partitioning strategies (list, range).
- Assumes PostgreSQL's standard internal seed and magic constants by default.

**Compatibility:**

- Ruby `>= 3.0.0`
- PostgreSQL `>= 11` (tested up to 16)

Note: PRs and support very much welcome

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'pg_hash_func'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install pg_hash_func

## Usage

**Example 1: Partitioning by bigint (e.g., User ID)**

```ruby
TABLE_PREFIX_BIGINT = "events"
USER_ID = 123_456_789_012_345 # bigint value
NUM_PARTITIONS_BIGINT = 16
index_bigint = PgHashFunc.calculate_partition_index_bigint(
  value: USER_ID,
  num_partitions: NUM_PARTITIONS_BIGINT
)

# Construct the partition table name
partition_name_bigint = [TABLE_PREFIX_BIGINT, index_bigint].join("_")

puts "User #{USER_ID} (bigint) belongs to partition: #{partition_name_bigint}"
# => User 123456789012345 (bigint) belongs to partition: events_14
```

**Example 2: Partitioning by integer (e.g., Tenant ID)**

```ruby
TABLE_PREFIX_INT = "tenant_data"
TENANT_ID = 987_654 # An integer value (fits in int4/int2)
NUM_PARTITIONS_INT = 32

# Calculate the index using the int4 function
# This also works correctly if TENANT_ID was a smallint
index_int = PgHashFunc.calculate_partition_index_int4(
  value: TENANT_ID,
  num_partitions: NUM_PARTITIONS_INT
)

partition_name_int = [TABLE_PREFIX_INT, index_int].join("_")

puts "Tenant 987654 (int) belongs to partition: tenant_data_28"
# => "tenant_data_22"
```

**Example 3: Two-Level Partitioning (bigint then int4)**

```ruby
TABLE_PREFIX_MULTI = "user_settings"
CUSTOMER_ID = 555_444_333_222_111 # bigint
SETTING_TYPE = 101                # integer
NUM_PARTITIONS_L1 = 64            # For CUSTOMER_ID
NUM_PARTITIONS_L2 = 8             # For SETTING_TYPE

# Calculate index for each level separately using the correct function
index_l1 = PgHashFunc.calculate_partition_index_bigint(value: CUSTOMER_ID, num_partitions: NUM_PARTITIONS_L1)
index_l2 = PgHashFunc.calculate_partition_index_int4(value: SETTING_TYPE, num_partitions: NUM_PARTITIONS_L2)

partition_name_multi = [TABLE_PREFIX_MULTI, index_l1, index_l2].join("_")

puts "Settings for Customer=#{CUSTOMER_ID}, Type=#{SETTING_TYPE} belong to: #{partition_name_multi}"
# => Settings for Customer=555444333222111, Type=101 belong to: user_settings_44_0
```

**Raw Hash Function**

Access the underlying PostgreSQL `hashint8extended` function directly. Primarily useful for debugging or specific integration scenarios.

```ruby
USER_ID = 123_456_789_012_345 # From Example 1

raw_hash_bigint = PgHashFunc.hashint8extended(value: USER_ID)

puts "Raw bigint hash for #{USER_ID}: #{raw_hash_bigint}"
# => Raw bigint hash for 123456789012345: 1245190300417211467
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `bundle exec rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`.

## Releasing

1.  Update the `VERSION` constant in `lib/pg_hash_func/version.rb`.
2.  Commit the changes.
3.  Run the release script, providing the version number:
    ```bash
    scripts/release.sh <VERSION>
    # e.g., scripts/release.sh 0.1.0
    ```
    This script will:
    - Build the gem.
    - Push the gem to RubyGems.
    - Create a git tag (e.g., `v0.1.0`).
    - Push the tag to GitHub.
    - Clean up the local gem file.
4.  Create a release on GitHub using the tag created.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/shayonj/pg_hash_func.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the PgHashFunc project's codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/shayonj/pg_hash_func/blob/master/CODE_OF_CONDUCT.md).
