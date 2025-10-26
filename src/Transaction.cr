require "./exceptions"
require "./Log"

module Lawn
  class Transaction
    EMPTY_VALUE = Bytes.new 0

    getter database : Database

    getter batches : Array(Array({Key, Value?}))
    getter accessed_keys : Set({UInt8, Key}) = Set({UInt8, Key}).new
    getter committed = false

    protected def initialize(@database)
      @batches = @database.tables.map { |table| Array({Key, Value?}).new }
    end

    def set(table_id : UInt8, key : Key, value : Value = EMPTY_VALUE)
      ::Log.debug { "#{self.class}.set table_id: #{table_id}, key: #{key.hexstring}, value: #{value.hexstring}" }
      @accessed_keys << {table_id, key}
      @batches[table_id] << {key, value}
      self
    end

    def set(table_id : UInt8, keyvalues : Array({Key, Value}))
      keyvalues.each { |key, value| set table_id, key, value }
      self
    end

    def delete(table_id : UInt8, key : Key)
      ::Log.debug { "#{self.class}.delete table_id: #{table_id}, key: #{key.hexstring}" }
      @accessed_keys << {table_id, key}
      @batches[table_id] << {key, nil}
      self
    end

    def get(table_id : UInt8, key : Key) : Value?
      ::Log.debug { "#{self.class}.get table_id: #{table_id}, key: #{key.hexstring}" }
      @accessed_keys << {table_id, key}
      @database.tables[table_id].get key
    end

    def commit
      ::Log.debug { "#{self.class}.commit" }
      raise Exception.new "Can not commit transaction which has already been committed" if @committed
      @committed = true
      @database.commit self
      @database
    end
  end
end
