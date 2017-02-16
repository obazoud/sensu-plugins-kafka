#! /usr/bin/env ruby
#
# metrics-consumer
#
# DESCRIPTION:
#   Gets consumers's offset, logsize and lag metrics from Kafka/Zookeeper and puts them in Graphite for longer term storage
#
# OUTPUT:
#   metric-data
#
# PLATFORMS:
#   Linux
#
# DEPENDENCIES:
#   gem: sensu-plugin
#
# USAGE:
#   #YELLOW
#
#
# LICENSE:
#   Copyright 2015 Olivier Bazoud
#   Released under the same terms as Sensu (the MIT license); see LICENSE
#   for details.
#

require 'sensu-plugin/metric/cli'

require 'json'
require 'poseidon'
require 'zookeeper'

class ConsumerOffsetMetrics < Sensu::Plugin::Metric::CLI::Graphite
  option :scheme,
         description: 'Metric naming scheme, text to prepend to metric',
         short: '-s SCHEME',
         long: '--scheme SCHEME',
         default: 'sensu.kafka.consumers'

  option :group,
         description: 'Consumer group',
         short:       '-g NAME',
         long:        '--group NAME',
         required:    true

  option :topic,
         description: 'Comma-separated list of consumer topics',
         short:       '-t NAME',
         long:        '--topic NAME'

  option :topic_excludes,
         description: 'Excludes consumer topics',
         short:       '-e NAME',
         long:        '--topic-excludes NAME',
         proc:        proc { |a| a.split(',') }

  option :zookeeper,
         description: 'ZooKeeper connect string',
         short:       '-z NAME',
         long:        '--zookeeper NAME',
         default:     'localhost:2181'

  def kafka_topics(zk, group)
    zk.get_children(path: "/consumers/#{group}/owners")[:children].sort
  end

  def topics_partitions(zk, topic)
    JSON.parse(zk.get(path: "/brokers/topics/#{topic}")[:data])['partitions'].keys.map(&:to_i).sort
  end

  def leader_broker(zk, topic, partition)
    state = zk.get(path: "/brokers/topics/#{topic}/partitions/#{partition}/state")
    leader = JSON.parse(state[:data])['leader']
    JSON.parse(zk.get(path: "/brokers/ids/#{leader}")[:data])
  end

  def consumer_offset(zk, group, topic, partition)
    zk.get(path: "/consumers/#{group}/offsets/#{topic}/#{partition}")[:data].to_i
  end

  def run
    z = Zookeeper.new(config[:zookeeper])

    group = config[:group]
    topics = kafka_topics(z, group)

    critical 'Could not found topics' if topics.empty?

    consumers = {}
    topics.each do |topic|
      consumers[topic] = []

      topics_partitions(z, topic).each do |partition|
        leader = leader_broker(z, topic, partition)
        consumer = Poseidon::PartitionConsumer.new('CheckConsumerLag', leader['host'], leader['port'], topic, partition, :latest_offset)
        logsize = consumer.next_offset

        offset = consumer_offset(z, group, topic, partition)

        lag = logsize - offset
        consumers[topic].push(partition: partition, logsize: logsize, offset: offset, lag: lag)
      end
    end

    [:offset, :logsize, :lag].each do |field|
      consumers.each do |k, v|
        critical "Topic #{k} has #{field} < 0 '#{v[field]}'" unless v.select { |w| w[field].to_i < 0 }.empty?
      end
    end

    [:offset, :logsize, :lag].each do |field|
      sum_by_group = consumers.map do |k, v|
        Hash[k, v.inject(0) { |a, e| a + e[field].to_i }]
      end
      sum_by_group.each do |x|
        output "#{config[:scheme]}.#{config[:group]}.#{x.keys[0]}.#{field}", x.values[0]
      end
    end
    ok
  rescue => e
    critical "Error: exception: #{e}"
  end
end
