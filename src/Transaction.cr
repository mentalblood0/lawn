require "./Log"

module Lawn
  class Transaction
    getter batch : Array({Key, Value?}) = Array({Key, Value?}).new

    protected def initialize(@env : Env)
    end

    def set(keyvalues : Array({Key, Value?}))
      ::Log.debug { "Transaction.set #{keyvalues.map { |key, value| {key.hexstring, value.hexstring} }}" }
      @batch.concat keyvalues
      self
    end

    def set(keyvalue : {Key, Value?})
      ::Log.debug { "Transaction.set #{keyvalue}" }
      @batch << keyvalue
      self
    end

    def set(key : Key, value : Value)
      ::Log.debug { "Transaction.set #{{key, value}}" }
      @batch << {key, value}
      self
    end

    def delete(keys : Array(Key))
      ::Log.debug { "Transaction.delete #{keys.map &.hexstring}" }
      keys.each { |key| @batch << {key, nil} }
      self
    end

    def delete(key : Key)
      ::Log.debug { "Transaction.delete #{key.hexstring}" }
      @batch << {key, nil}
      self
    end

    def commit
      ::Log.debug { "Transaction.commit" }
      @env.log.write @batch
      @batch.each { |key, value| @env.memtable[key] = value }
      @env
    end
  end
end
