require 'json'

# encoding: utf-8
module SensuPluginsKafka
  # This defines the version of the gem
  module Version
    MAJOR = 0
    MINOR = 12
    PATCH = 0

    VER_STRING = [MAJOR, MINOR, PATCH].compact.join('.')
  end
end
