# frozen_string_literal: true

require "bundler/gem_tasks"
require "rspec/core/rake_task"
require "rubocop/rake_task"

# Run specs
RSpec::Core::RakeTask.new(:spec)

# Run RuboCop
RuboCop::RakeTask.new(:rubocop)

# Default task: run specs and RuboCop
task default: %i[spec rubocop]
