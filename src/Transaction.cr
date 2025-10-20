require "./Log"

module Lawn
  class Transaction
    getter batches : Array(Array({Key, Value?})?) = Array(Array({Key, Value?})?).new(256) { nil }

    protected def initialize(@database : Database)
    end

    def batch(table_id : UInt8)
      batches[table_id] = Array({Key, Value?}).new unless batches[table_id]
      batches[table_id].not_nil!
    end

    def set(table_id : UInt8, keyvalues : Array({Key, Value?}))
      ::Log.debug { "Transaction.set table_id: #{table_id}, keyvalues: #{keyvalues.map { |key, value| {key.hexstring, value.hexstring} }}" }
      batch(table_id).concat keyvalues
      self
    end

    def set(table_id : UInt8, keyvalue : {Key, Value?})
      ::Log.debug { "Transaction.set table_id: #{table_id}, keyvalue: #{{keyvalue[0].hexstring, keyvalue[1] ? keyvalue[1].hexstring : nil}}" }
      batch(table_id) << keyvalue
      self
    end

    def set(table_id : UInt8, key : Key, value : Value)
      ::Log.debug { "Transaction.set table_id: #{table_id}, key: #{key.hexstring}, value: #{value.hexstring}" }
      batch(table_id) << {key, value}
      self
    end

    def delete(table_id : UInt8, keys : Array(Key))
      ::Log.debug { "Transaction.delete table_id: #{table_id}, keys: #{keys.map &.hexstring}" }
      batch(table_id) { |key| @batch << {key, nil} }
      self
    end

    def delete(table_id : UInt8, key : Key)
      ::Log.debug { "Transaction.delete table_id: #{table_id}, key: #{key.hexstring}" }
      batch(table_id) << {key, nil}
      self
    end

    def commit
      ::Log.debug { "Transaction.commit" }
      @batches.each_with_index do |batch, table_id|
        next unless batch
        @database.log.write table_id.to_u8, batch
        table = @database.tables[table_id]
        batch.each { |key, value| table.memtable[key] = value }
        @database.tables[table_id]
      end
      @database
    end
  end
end
