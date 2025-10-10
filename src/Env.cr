require "yaml"

require "./common"
require "./Transaction"
require "./Log"
require "./SplitDataStorage"

module Lawn
  class Env
    Lawn.mserializable

    getter log : Log
    getter split_data_storage : SplitDataStorage

    @[YAML::Field(ignore: true)]
    getter h : Hash(Bytes, Bytes?) = Hash(Bytes, Bytes?).new

    def after_initialize
      @log.read { |kv| @h[kv[0]] = kv[1] }
    end

    def checkpoint
      return self if @h.empty?

      @log.truncate
      @h.clear
      self
    end

    def transaction
      Transaction.new self
    end

    def get(k : Bytes)
      return @h[k]?
    end
  end
end
