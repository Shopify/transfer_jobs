require 'bundler/gem_tasks'
require 'sidekiq'
require 'sidekiq-unique-jobs'

import './lib/tasks/sidekiq.rake'

require 'rake/testtask'

Rake::TestTask.new do |t|
  t.libs = %w(lib test)
  t.test_files = FileList['test/**/*_test.rb']
end

desc "Run tests"
task :test
