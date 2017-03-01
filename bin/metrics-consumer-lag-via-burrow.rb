#!/usr/bin/env ruby
#
# metrics-consumer-lag-via-burrow
#
# DESCRIPTION:
#   Stores kafka consumers lag in Graphite.
#   Fetches stats from linkedin/Burrow and ZooKeeper.
#
# OUTPUT:
#   metric-data
#
# PLATFORMS:
#   Linux
#
# DEPENDENCIES:
#   gem: sensu-plugin
#   gem: zookeeper
#
# USAGE:
#   ./metrics-consumer-lag-via-burrow.rb \
#     --zookeeper_nodes 'zookeeper1.example.com:2181,zookeeper2.example.com:2181' \
#     --burrow_url 'http://burrow.example.com:8000' \
#     --event_group 'event-group' \
#     --kafka_cluster 'main' \
#     --scheme 'kafka.consumers.lag'

require 'sensu-plugin/metric/cli'

class CheckKafkaConsumerLag < Sensu::Plugin::Metric::CLI::Graphite
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

  option :scheme,
         description: 'Metric naming scheme, text to prepend to metric',
         short: '-s SCHEME',
         long: '--scheme SCHEME',
         required: false,
         default: 'sensu_kafka.consumers.lag'

  def run
    output(key_for('total_lag'), consumer_stats[:total_lag], now)
    consumer_stats[:partitions].each do |partition|
      output(key_for(partition[:owner]), partition[:lag], now)
    end

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

  def key_for(partition)
    "#{config[:scheme]}.#{partition.tr('.-', '_')}"
  end

  def now
    @now ||= Time.now.to_i
  end
end
