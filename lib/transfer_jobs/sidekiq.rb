module TransferJobs
  module Sidekiq
    require 'sidekiq/api'
    require 'transfer_jobs/sidekiq_mover'
    require 'transfer_jobs/sidekiq_queue'
    require 'transfer_jobs/sidekiq_delayed_queue'
  end
end
