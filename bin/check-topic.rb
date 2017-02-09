#! /usr/bin/env ruby
#
# check-topic
#
# DESCRIPTION:
#   This plugin checks topic properties.
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
#   ./check-topic
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
require 'zookeeper'

class TopicsCheck < Sensu::Plugin::Check::CLI
  option :zookeeper,
         description: 'ZooKeeper connect string (host:port,..)',
         short:       '-z ZOOKEEPER',
         long:        '--zookeeper ZOOKEEPER',
         default:     'localhost:2181',
         required:    true

  option :name,
         description: 'Topic name',
         short: '-n TOPIC_NAME',
         long: '--name TOPIC_NAME',
         required:    true

  option :partitions,
         description: 'Partitions',
         short: '-p PARTITIONS_COUNT',
         long: '--partitions TOPIC_NAME',
         proc: proc(&:to_i)

  option :replication_factor,
         description: 'Replication factor',
         short: '-r REPLICATION_FACTOR',
         long: '--replication-factor REPLICATION_FACTOR',
         proc: proc(&:to_i)

  def run
    z = Zookeeper.new(config[:zookeeper])

    topics = z.get_children(path: '/brokers/topics')[:children].sort

    critical "Topic '#{config[:name]}' not found" unless topics.include? config[:name]

    if config.key?(:partitions) || config.key?(:replication_factor)
      partitions_data = z.get(path: "/brokers/topics/#{config[:name]}")[:data]
      partitions = JSON.parse(partitions_data)['partitions']

      critical "Topic '#{config[:name]}' has #{partitions.size} partitions, expecting #{config[:partitions]}" if config.key?(:partitions) && partitions.size != config[:partitions]

      if config.key?(:replication_factor)
        min = partitions.min_by { |_, brokers| brokers.size }[1].length
        max = partitions.max_by { |_, brokers| brokers.size }[1].length
        critical "Topic '#{config[:name]}' RF is between #{min} and #{max}, expecting #{config[:replication_factor]}" if config[:replication_factor] != min || min != max
      end
    end
    ok
  rescue => e
    puts "Error: #{e.backtrace}"
    critical "Error: #{e}"
  end
end
