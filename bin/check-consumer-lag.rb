#! /usr/bin/env ruby
#
# check-consumer-lag
#
# DESCRIPTION:
#   This plugin checks the lag of your kafka's consumers.
#
# OUTPUT:
#   plain-text
#
# PLATFORMS:
#   Linux
#
# DEPENDENCIES:
#   gem: sensu-plugin
#
# USAGE:
#   ./check-consumer-lag
#
# NOTES:
#
# LICENSE:
#   Olivier Bazoud
#   Released under the same terms as Sensu (the MIT license); see LICENSE
#   for details.
#

require 'sensu-plugin/check/cli'

require 'json'
require 'poseidon'
require 'zookeeper'

class ConsumerLagCheck < Sensu::Plugin::Check::CLI
  option :group,
         description: 'Consumer group',
         short:       '-g NAME',
         long:        '--group NAME',
         required:    true

  option :zookeeper,
         description: 'ZooKeeper connect string',
         short:       '-z NAME',
         long:        '--zookeeper NAME',
         default:     'localhost:2181'

  option :warning_over,
         description: 'Warning if lag is over specified value.',
         short:       '-W N',
         long:        '--warning-over N'

  option :critical_over,
         description: 'Critical if lag is over specified value.',
         short:       '-C N',
         long:        '--critical-over N'

  option :warning_under,
         description: 'Warning if lag is under specified value.',
         short:       '-w N',
         long:        '--warning-under N'

  option :critical_under,
         description: 'Critical if lag is under specified value.',
         short:       '-c N',
         long:        '--critical-under N'

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

  def partition_owner(zk, group, topic, partition)
    zk.get(path: "/consumers/#{group}/owners/#{topic}/#{partition}")[:data]
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
        owner = partition_owner(z, group, topic, partition)

        leader = leader_broker(z, topic, partition)
        consumer = Poseidon::PartitionConsumer.new('CheckConsumerLag', leader['host'], leader['port'], topic, partition, :latest_offset)
        logsize = consumer.next_offset

        offset = consumer_offset(z, group, topic, partition)

        lag = logsize - offset
        consumers[topic].push(partition: partition, logsize: logsize, offset: offset, lag: lag, owner: owner)
      end
    end

    [:offset, :logsize, :lag].each do |field|
      consumers.each do |k, v|
        critical "Topic #{k} has #{field} < 0 '#{v[field]}'" unless v.select { |w| w[field].to_i < 0 }.empty?
      end
    end

    consumers.each do |k, v|
      critical "Topic #{k} has partitions with no owner '#{v[:owner]}'" unless v.select { |w| w[:owner] == 'none' }.empty?
    end

    lags = consumers.map do |k, v|
      Hash[k, v.inject(0) { |a, e| a + e[:lag] }]
    end

    max_lag = lags.map(&:values).flatten.max
    max_topics = lags.select { |a| a.key(max_lag) }.map(&:keys).flatten

    min_lag = lags.map(&:values).flatten.min
    min_topics = lags.select { |a| a.key(min_lag) }.map(&:keys).flatten

    # Global
    [:over, :under].each do |over_or_under|
      [:critical, :warning].each do |severity|
        threshold = config[:"#{severity}_#{over_or_under}"]

        next unless threshold
        case over_or_under
        when :over
          if max_lag > threshold.to_i
            msg = "Topics `#{max_topics}` for the group `#{config[:group]}` lag: #{max_lag} (>= #{threshold})"
            send severity, msg
          end
        when :under
          if min_lag < threshold.to_i
            msg = "Topics `#{min_topics}` for the group `#{config[:group]}` lag: #{min_lag} (<= #{threshold})"
            send severity, msg
          end
        end
      end
    end

    # Per partition
    [:over, :under].each do |over_or_under|
      [:critical, :warning].each do |severity|
        threshold = config[:"#{severity}_#{over_or_under}"]

        next unless threshold

        consumers.each do |k, v|
          v.each do |partition|
            case over_or_under
            when :over
              if partition[:lag].to_f > (threshold.to_f * 1.33 / v.size)
                msg = "Topics `#{k}` partition #{partition[:partition]} lag: #{partition[:lag]} (>= #{threshold.to_f * 1.33 / v.size})"
                send severity, msg
              end
            when :under
              if min_lag < (threshold.to_f / v.size)
                msg = "Topics `#{k}` partition #{partition[:partition]} lag: #{partition[:lag]} (<= #{threshold.to_f / v.size})"
                send severity, msg
              end
            end
          end
        end
      end
    end

    ok "Group `#{config[:group]}`'s lag is ok (#{min_lag}/#{max_lag})"

  rescue => e
    puts "Error: exception: #{e} - #{e.backtrace}"
    critical
  end
end
