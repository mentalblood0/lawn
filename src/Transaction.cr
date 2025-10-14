require "./Log"

module Lawn
  class Transaction
    getter batch : Hash(Bytes, Bytes?) = Hash(Bytes, Bytes?).new

    protected def initialize(@env : Env)
    end

    def set(kvs : Hash(K, V))
      ::Log.debug { "Transaction.set #{kvs.map { |k, v| {k.hexstring, v.hexstring} }}" }
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
      ::Log.debug { "Transaction.delete #{ks.map &.hexstring}" }
      ks.each { |k| @batch[k] = nil }
      self
    end

    def delete(k : K)
      delete [k]
    end

    def commit
      ::Log.debug { "Transaction.commit" }
      @env.log.write @batch
      @env.memtable.merge! @batch
      @env
    end
  end
end
