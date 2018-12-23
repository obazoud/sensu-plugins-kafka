#! /usr/bin/env ruby
#
# check-broker
#
# DESCRIPTION:
#   This plugin checks broker properties.
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
#   ./check-broker
#
# NOTES:
#
# LICENSE:
#   Olivier Bazoud
#   Released under the same terms as Sensu (the MIT license); see LICENSE
#   for details.
#

require 'sensu-plugin/check/cli'
require 'zookeeper'

class BrokerCheck < Sensu::Plugin::Check::CLI
  option :zookeeper,
         description: 'ZooKeeper connect string (host:port,..)',
         short:       '-z ZOOKEEPER',
         long:        '--zookeeper ZOOKEEPER',
         default:     'localhost:2181',
         required:    true

  option :ids,
         description: 'Brokers ids',
         short:       '-i IDS',
         long:        '--ids IDS',
         proc:        proc { |a| a.split(',') },
         required:    true

  option :size,
         description: 'Broker size',
         short: '-s BROKER_SIZE',
         long: '--broker-size BROKER_SIZE',
         proc: proc(&:to_i)
  def run
    z = Zookeeper.new(config[:zookeeper])

    brokers = z.get_children(path: '/brokers/ids')[:children] 

    critical "Broker '#{config[:ids] - brokers}' not found" unless (config[:ids] - brokers).length == 0
    critical "Broker wrong size: #{brokers.length} (#{brokers}), expecting #{config[:size]}, missing #{brokers- config[:ids]}" unless brokers.length == config[:size]

    ok
  rescue => e
    puts "Error: #{e.backtrace}"
    critical "Error: #{e}"
  end
end
