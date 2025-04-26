# frozen_string_literal: true

source "https://rubygems.org"

git_source(:github) { |repo_name| "https://github.com/#{repo_name}" }

gemspec

group :development, :test do
  gem "benchmark-ips"
  gem "bundler"
  gem "pg"
  gem "rake", "~> 13.0"
  gem "rspec", "~> 3.0"
  gem "rubocop", "~> 1.60" # Use a recent version
  gem "rubocop-performance"
  gem "rubocop-rake"
  gem "rubocop-rspec"
end
