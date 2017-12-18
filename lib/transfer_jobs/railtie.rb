class TransferJobs::Railtie < Rails::Railtie
  rake_tasks do
    load 'tasks/sidekiq.rake' if defined?(Sidekiq)
    load 'tasks/resque.rake' if defined?(Resque)
  end
end
