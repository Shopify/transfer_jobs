module TransferJobs
  require 'transfer_jobs/redis_queue'
  require 'transfer_jobs/multi_queue_mover'
  require 'transfer_jobs/redis_job_set'
  require 'transfer_jobs/util'

  require 'transfer_jobs/railtie' if defined?(Rails)
end
