require 'uri'
require 'open-uri'
require 'json'
require 'timeout'
require 'socket'
require 'zookeeper'

class ConsumerLagBurrowStats
  def self.for(*args)
    new(*args).stats
  end

  attr_reader :zookeeper_nodes, :burrow_url, :event_group, :kafka_cluster, :burrow_timeout

  def initialize(zookeeper_nodes:, burrow_url:, burrow_timeout:, event_group:, kafka_cluster:)
    @zookeeper_nodes = zookeeper_nodes
    @burrow_url = burrow_url
    @burrow_timeout = burrow_timeout
    @event_group = event_group
    @kafka_cluster = kafka_cluster
  end

  def stats
    {
      total_lag: total_lag,
      partitions: partition_stats,
      url: consumers_check_url.to_s
    }
  end

  private

  def total_lag
    burrow_stats[:totallag]
  end

  def burrow_stats
    @burrow_stats ||= fetch_burrow_stats[:status]
  end

  def fetch_burrow_stats
    Timeout.timeout(burrow_timeout) do
      # http request has #read_timeout, but does not have connect_timeout
      body = JSON.parse(open(consumers_check_url).read, symbolize_names: true)
      raise "failed fetching #{consumers_check_url} - #{body[:message]}" if body[:error]

      body
    end
  rescue SocketError => e
    raise "failed fetching #{consumers_check_url} - #{e.message}"
  rescue Timeout::Error
    raise "timed-out after #{burrow_timeout}s - #{consumers_check_url}, " \
          'ensure that host is reachable and url parameters are correct'
  rescue JSON::ParserError => e
    raise "failed parsing #{consumers_check_url} - #{e.message}"
  end

  def consumers_check_url
    @consumers_check_url ||= URI.join(
      burrow_url,
      "v2/kafka/#{kafka_cluster}/consumer/#{event_group}/lag"
    )
  end

  def partition_stats
    burrow_stats[:partitions].map do |partition|
      {
        id: partition[:partition],
        lag: partition[:end][:lag],
        owner: partition_owner(partition)
      }
    end
  end

  def partition_owner(partition)
    url = "/kafka/consumers/#{event_group}/owners/#{partition[:topic]}/#{partition[:partition]}"
    zookeeper
      .get(path: url)[:data]
      .split(/-\d{13}-/).first # ignore timestamp in owner name
  end

  def zookeeper
    @zookeeper ||= Zookeeper.new(zookeeper_nodes)
  end
end
