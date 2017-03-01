## Sensu-Plugins-Kafka

[![Build Status](https://travis-ci.org/obazoud/sensu-plugins-kafka.svg?branch=master)](https://travis-ci.org/obazoud/sensu-plugins-kafka) [![Gem Version](https://badge.fury.io/rb/sensu-plugins-kafka.svg)](https://badge.fury.io/rb/sensu-plugins-kafka)

## Functionality

Tracks and reports Kafka consumers metrics.

There are two different data sources -
[Kafka scripts](https://en.wikipedia.org/wiki/Apache_Kafka)
and [Burrow API](https://github.com/linkedin/Burrow).

Checks ending with `via-burrow` use Burrow as a data source.

## Files

* checks via Kafka
  * [check-consumer-lag.rb](bin/check-consumer-lag.rb)
  * [check-topic.rb](bin/check-topic.rb)
  * [check-topics-name.rb](bin/check-topics-name.rb)
  * [metrics-consumer.rb](bin/metrics-consumer.rb)
* checks via Burrow
  * [check-consumer-lag-via-burrow.rb](bin/check-consumer-lag-via-burrow.rb)
  * [metrics-consumer-lag-via-burrow](bin/metrics-consumer-lag-via-burrow.rb)

## Usage

## Installation

[Installation and Setup](http://sensu-plugins.io/docs/installation_instructions.html)

Tested on:
* Zookeeper version: 3.4.6
* Kafka version: 0.8.2.x

Note: In addition to the standard installation requirements the installation of this gem will require compiling the nokogiri gem.
Due to this you'll need certain developmemnt packages on your system.
On Ubuntu systems install build-essential, libxml2-dev and zlib1g-dev.
On CentOS install gcc and zlib-devel.
