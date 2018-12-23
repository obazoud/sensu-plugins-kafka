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
require 'json'
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
         short:       '-n IDS',
         long:        '--name IDS',
         proc:        proc { |a| a.split(',') },
         required:    true

  def run
    z = Zookeeper.new(config[:zookeeper])

    brokers = z.get(path: '/brokers')[:data]

    critical "Broker '#{brokers}' not found"

    ok
  rescue => e
    puts "Error: #{e.backtrace}"
    critical "Error: #{e}"
  end
end
