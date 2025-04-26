# frozen_string_literal: true

require_relative "lib/pg_hash_func/version"

Gem::Specification.new do |spec|
  spec.name          = "pg_hash_func"
  spec.version       = PgHashFunc::VERSION
  spec.authors       = ["Shayon Mukherjee"]
  spec.email         = ["shayonj@gmail.com"]

  spec.summary       = "Determine the target partition index for an integer key according " \
                       "to PostgreSQL's default hash strategy, without querying the database."
  spec.description   = <<~DESC
    Replicates PostgreSQL's default hash partitioning calculations.
    Specifically targets the logic within `hashint8extended` (for bigint)
    and `hashint4extended` (for integer/smallint) from PostgreSQL's
    `src/backend/access/hash/hashfunc.c`.
  DESC
  spec.homepage      = "https://github.com/shayonj/pg_hash_func"
  spec.license       = "MIT"
  spec.required_ruby_version = ">= 3.0.0"

  if spec.respond_to?(:metadata)
    spec.metadata["homepage_uri"] = spec.homepage
    spec.metadata["source_code_uri"] = "https://github.com/shayonj/pg_hash_func"
    spec.metadata["changelog_uri"] = "https://github.com/shayonj/pg_hash_func/blob/main/CHANGELOG.md"
  else
    raise "RubyGems 2.0 or newer is required to protect against " \
          "public gem pushes."
  end

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject do |f|
      f.match(%r{\A(?:(?:bin|test|spec|features)/|\.(?:git|travis|circleci)|appveyor)})
    end
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
  spec.metadata["rubygems_mfa_required"] = "true"
end
