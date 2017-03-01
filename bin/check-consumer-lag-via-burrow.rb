#!/usr/bin/env ruby
#
# check-consumer-lag-via-burrow
#
# DESCRIPTION:
#   This plugin checks the lag of your kafka's consumers.
#   It fetches stats from linkedin/Burrow and ZooKeeper.
#
# OUTPUT:
#   plain-text
#
# PLATFORMS:
#   Linux
#
# DEPENDENCIES:
#   gem: sensu-plugin
#   gem: zookeeper
#
# USAGE:
#   ./check-consumer-lag-via-burrow.rb \
#     --zookeeper_nodes 'zookeeper1.example.com:2181,zookeeper2.example.com:2181' \
#     --burrow_url 'http://burrow.example.com:8000' \
#     --event_group 'event-group' \
#     --kafka_cluster 'main' \
#     --warning_threshold 100000 \
#     --critical_threshold 1000000

require 'sensu-plugin/check/cli'
require_relative 'consumer_lag_burrow_stats'

class CheckConsumerLagViaBurrow < Sensu::Plugin::Check::CLI
  option :zookeeper_nodes,
         description: 'Comma separated ZooKeeper nodes',
         short: '-z nodes',
         long: '--zookeeper_nodes NODES',
         required: true

  option :burrow_url,
         description: 'linkedin/Burrow host url',
         short: '-u URL',
         long: '--burrow_url URL',
         required: true

  option :burrow_timeout,
         description: 'Burrow request timeout',
         short: '-t SECONDS',
         long: '--burrow_timeout SECONDS',
         default: 2

  option :event_group,
         description: 'Event group name',
         short: '-g GROUP',
         long: '--event_group GROUP',
         required: true

  option :kafka_cluster,
         description: 'Kafka cluster name',
         short: '-c CLUSTER',
         long: '--kafka_cluster CLUSTER',
         required: true

  option :warning_threshold,
         description: 'Total event group consumers lag warning threshold, zero for none',
         short: '-w WARNING',
         long: '--warning_threshold WARNING',
         default: 0

  option :critical_threshold,
         description: 'Total event group consumers lag critical threshold, zero for none',
         short: '-c CRITICAL',
         long: '--critical_threshold CRITICAL',
         default: 0

  def run
    critical(lag_message) if exceeds?(critical_threshold)
    warning(lag_message) if exceeds?(warning_threshold)

    ok
  rescue RuntimeError => e
    critical e
  end

  private

  def consumer_stats
    @consumer_stats ||= ConsumerLagBurrowStats.for(
      zookeeper_nodes: config[:zookeeper_nodes],
      burrow_url: config[:burrow_url],
      burrow_timeout: config[:burrow_timeout].to_i,
      event_group: config[:event_group],
      kafka_cluster: config[:kafka_cluster]
    )
  end

  def warning_threshold
    @warning_threshold ||= config[:warning_threshold].to_i
  end

  def critical_threshold
    @critical_threshold ||= config[:critical_threshold].to_i
  end

  def exceeds?(threshold)
    threshold > 0 && consumer_stats[:total_lag] > threshold
  end

  def lag_message
    ([total_lag_message] + partition_messages).join("\n")
  end

  def total_lag_message
    [
      config[:event_group],
      'is',
      in_milions(consumer_stats[:total_lag]),
      'events behind -',
      consumer_stats[:url]
    ].join(' ')
  end

  def partition_messages
    consumer_stats[:partitions]
      .sort_by { |partition| -partition[:lag] }
      .map do |partition|
        [
          in_milions(partition[:lag]),
          'lag for',
          partition[:owner],
          "(partition #{partition[:id]})"
        ].join(' ')
      end
  end

  def in_milions(number)
    "#{(number.to_f / 1_000_000).round(3)} Millions"
  end
end
