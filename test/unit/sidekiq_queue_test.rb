require 'test_helper'

class TransferJobs::SidekiqQueueTest < TransferJobsTestCase
  include JobHelper

  def test_iterates_over_jobs
    SimpleJob.perform_async(arg: 1)
    SimpleJob.perform_async(arg: 2)
    SimpleJob.perform_async(arg: 3)

    redis = Sidekiq.redis do |redis|
      redis
    end

    queue = TransferJobs::SidekiqQueue.new(redis: redis, key: 'queue:default')
    items = []

    queue.in_batches do |batch|
      items += batch
    end

    assert_equal [1,2,3], items.map { |item| item.args[0]['arg'] }.sort
  end

  def test_batches_properly
    SimpleJob.perform_async(arg: 1)
    SimpleJob.perform_async(arg: 2)
    SimpleJob.perform_async(arg: 3)

    redis = Sidekiq.redis do |redis|
      redis
    end

    queue = TransferJobs::SidekiqQueue.new(redis: redis, batch_size: 1, key: 'queue:default')
    items = []
    batch_count = 0
    queue.in_batches do |batch|
      batch_count += 1
      items += batch
    end

    assert_equal 3, batch_count
    assert_equal [1,2,3], items.map { |item| item.args[0]['arg'] }.sort
  end
end
