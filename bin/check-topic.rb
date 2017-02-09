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

  def run
    z = Zookeeper.new(config[:zookeeper])

    live_topics = z.get_children(path: '/brokers/topics')[:children].sort

    critical "#{config[:name]} not found" unless live_topics.include? config[:name]

    ok
  rescue => e
    puts "Error: #{e.backtrace}"
    critical "Error: #{e}"
  end
end
