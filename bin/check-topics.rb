#! /usr/bin/env ruby
#
# check-topics
#
# DESCRIPTION:
#   This plugin checks topics properties.
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

class TopicCheck < Sensu::Plugin::Check::CLI
  option :kafka_home,
         description: 'Kafka home',
         short:       '-k NAME',
         long:        '--kafka-home NAME',
         default:     '/opt/kafka'

  option :zookeeper,
         description: 'ZooKeeper connect string',
         short:       '-z NAME',
         long:        '--zookeeper NAME',
         default:     'localhost:2181'

  option :properites,
         description: 'Replication factor for this topic',
         short:       '-R N',
         long:        '--replication-factor N'

  # read the output of a command
  # @param cmd [String] the command to read the output from
  def read_lines(cmd)
    IO.popen(cmd + ' 2>&1') do |child|
      child.read.split("\n")
    end
  end

  # create a hash from the output of each line of a command
  # @param line [String]
  # @param cols
  def line_to_hash(line, *cols)
    Hash[cols.zip(line.strip.split(/\s+/, cols.size))]
  end

  # run command and return a hash from the output
  # @param cms [String]
  def run_cmd(cmd)
    read_lines(cmd).drop(1).map do |line|
      line_to_hash(line, :_, :topic, :_, :partition, :_, :leader, :_, :replicas, :_, :isr)
    end
  end

  def run
    kafka_topics = "#{config[:kafka_home]}/bin/kafka-topics.sh"
    unknown "Can not find #{kafka_topics}" unless File.exist?(kafka_topics)

    cmd_details = "#{kafka_topics} --describe --zookeeper #{config[:zookeeper]} | grep -v ReplicationFactor"
    cmd_global = "#{kafka_topics} --describe --zookeeper #{config[:zookeeper]} | grep ReplicationFactor"

    results = run_cmd(cmd_details)

    topics = results.group_by { |h| h[:topic] }

# {:_=>"Isr:", :topic=>"__consumer_offsets", :partition=>"49", :leader=>"172314554", :replicas=>"172314554,172314557,172314558", :isr=>"172314554,172314557,172314558"}

    topics.each |name, topic| do
      topic.inject(0) { |a, e| [a, e[:replicas].length].max }
      end
    end
    ok

  rescue => e
    puts "Error: #{e}"
    puts "Error: #{e.backtrace}"
    critical "Error: #{e}"
  end
end
