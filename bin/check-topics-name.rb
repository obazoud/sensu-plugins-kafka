#! /usr/bin/env ruby
#
# check-topics-name
#
# DESCRIPTION:
#   This plugin checks topic's name.
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
#   ./check-topics-name
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
         short:       '-n TOPIC_NAME',
         long:        '--name TOPIC_NAME',
         proc:        proc { |a| a.split(',') },
         required:    true

  def run
    z = Zookeeper.new(config[:zookeeper])

    topics = z.get_children(path: '/brokers/topics')[:children].sort

    critical "Topics '#{config[:name] - topics}' not found" unless (config[:name] - topics).empty?
    critical "Topics '#{topics - config[:name]}' not checked" unless (topics - config[:name]).empty?

    ok
  rescue => e
    puts "Error: #{e.backtrace}"
    critical "Error: #{e}"
  end
end
