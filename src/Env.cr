require "yaml"

require "./common"
require "./Log"

module Lawn
  class Transaction
    getter batch : Hash(Bytes, Bytes?) = Hash(Bytes, Bytes?).new

    protected def initialize(@env : Env)
    end

    def set(kvs : Hash(K, V))
      @batch.merge! kvs
      self
    end

    def set(kv : KV)
      set({kv[0] => kv[1]})
    end

    def set(k : K, v : V)
      set({k => v})
    end

    def delete(ks : Enumerable(K))
      ks.each { |k| @batch[k] = nil }
      self
    end

    def delete(k : K)
      delete [k]
    end

    def commit
      @env.log.write @batch
      @env.h.merge! @batch
      @env
    end
  end

  class Env
    Lawn.mserializable

    getter log : Log

    @[YAML::Field(ignore: true)]
    getter h : Hash(Bytes, Bytes?) = Hash(Bytes, Bytes?).new

    def after_initialize
      @log.read { |k, v| @h[k] = v }
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
