# frozen_string_literal: true
require 'test_helper'

class TransferJobs::SidekiqDelayedQueueTest < TransferJobsTestCase
  include JobHelper

  def test_iterates_over_scheduled_timestamps
    base_time = nil
    Timecop.freeze do
      base_time = Time.now
      SimpleJob.perform_in(60)
      SimpleJob.perform_in(120)
      SimpleJob.perform_in(180)
    end

    redis = Sidekiq.redis do |redis|
      redis
    end

    scheduled_set = TransferJobs::SidekiqDelayedQueue.new(redis: redis)
    items = []

    scheduled_set.in_batches do |batch|
      items += batch
    end

    assert_equal [60, 120, 180].map { |time| (base_time + time).to_f }, items.map(&:last)
  end

  def test_iterates_batches_properly
    base_time = nil
    Timecop.freeze do
      base_time = Time.now
      SimpleJob.perform_in(60)
      SimpleJob.perform_in(120)
      SimpleJob.perform_in(180)
    end

    redis = Sidekiq.redis do |redis|
      redis
    end

    scheduled_set = TransferJobs::SidekiqDelayedQueue.new(redis: redis, batch_size: 1)
    items = []

    scheduled_set.in_batches do |batch|
      items += batch
    end

    assert_equal [60, 120, 180].map { |time| (base_time + time).to_f }, items.map(&:last)
  end

end
