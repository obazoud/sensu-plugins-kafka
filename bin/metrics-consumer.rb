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

  option :kafka_home,
         description: 'Kafka home',
         short:       '-k NAME',
         long:        '--kafka-home NAME',
         default:     '/opt/kafka'

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
      line_to_hash(line, :group, :topic, :pid, :offset, :logsize, :lag, :owner)
    end
  end

  def run
    begin
      kafka_run_class = "#{config[:kafka_home]}/bin/kafka-run-class.sh"
      unknown "Can not find #{kafka_run_class}" unless File.exist?(kafka_run_class)

      cmd = "#{kafka_run_class} kafka.tools.ConsumerOffsetChecker --group #{config[:group]} --zookeeper #{config[:zookeeper]}"
      cmd += " --topic #{config[:topic]}" if config[:topic]

      results = run_cmd(cmd)

      [:offset, :logsize, :lag].each do |field|
        sum_by_group = results.group_by { |h| h[:topic] }.map do |k, v|
          Hash[k, v.inject(0) { |a, e| a + e[field].to_i }]
        end
        sum_by_group.delete_if { |x| config[:topic_excludes].include?(x.keys[0]) } if config[:topic_excludes]
        sum_by_group.each do |x|
          output "#{config[:scheme]}.#{config[:group]}.#{x.keys[0]}.#{field}", x.values[0]
        end
      end
    rescue => e
      critical "Error: exception: #{e}"
    end
    ok
  end
end
