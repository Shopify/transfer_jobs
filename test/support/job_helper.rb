module JobHelper
  def self.included(base)
    def job_result_queue=(other)
      @job_result_queue = other
    end

    def job_result_queue
      @job_result_queue
    end
  end

  class SimpleJob
    include Sidekiq::Worker
    def perform(*args)
      self.job_result_queue << args
    end
  end
end
