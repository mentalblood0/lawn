require "./exceptions"
require "./Log"

module Lawn
  class Transaction
    EMPTY_VALUE = Bytes.new 0

    getter database : Database

    getter changes : Array(AVLTree)
    getter accessed_keys = {read: Set({UInt8, Key}).new, write: Set({UInt8, Key}).new}
    getter began_at : Time
    getter committed_at : Time?

    protected def initialize(@database)
      @changes = @database.tables.map { |table| AVLTree.new }
      @began_at = Time.utc
    end

    protected def check_write_interference(table_id : UInt8, key : Key)
      @database.transactions[:committed].each do |committed_transaction|
        if (@began_at < committed_transaction[:committed_at]) &&
           (
             committed_transaction[:accessed_keys][:write].includes?({table_id, key}) ||
             committed_transaction[:accessed_keys][:read].includes?({table_id, key})
           )
          raise Exception.new "Transaction #{self} try to get value by key #{key}, but value at this key was changed during this transaction by another committed transaction, so thi transaction can not perform this get"
        end
      end
    end

    def set(table_id : UInt8, key : Key, value : Value = EMPTY_VALUE)
      ::Log.debug { "#{self.class}.set table_id: #{table_id}, key: #{key.hexstring}, value: #{value.hexstring}" }
      check_write_interference table_id, key
      @accessed_keys[:write] << {table_id, key}
      @changes[table_id][key] = value
      self
    end

    def set(table_id : UInt8, keyvalues : Array({Key, Value}))
      keyvalues.each { |key, value| set table_id, key, value }
      self
    end

    def delete(table_id : UInt8, key : Key)
      ::Log.debug { "#{self.class}.delete table_id: #{table_id}, key: #{key.hexstring}" }
      check_write_interference table_id, key
      @accessed_keys[:write] << {table_id, key}
      @changes[table_id][key] = nil
      self
    end

    def get(table_id : UInt8, key : Key) : Value?
      ::Log.debug { "#{self.class}.get table_id: #{table_id}, key: #{key.hexstring}" }

      @database.transactions[:committed].each do |committed_transaction|
        if (@began_at < committed_transaction[:committed_at]) &&
           committed_transaction[:accessed_keys][:write].includes?({table_id, key})
          raise Exception.new "Transaction #{self} try to get value by key #{key}, but value at this key was changed during this transaction by another committed transaction, so thi transaction can not perform this get"
        end
      end
      @accessed_keys[:read] << {table_id, key}

      result = @changes[table_id][key]?
      return result if !result.is_a? Symbol

      @database.tables[table_id].get key
    end

    def cursor(table_id : UInt8, from : Key? = nil, including_from : Bool = true, direction = :forward)
      @database.tables[table_id].cursor @changes[table_id], from, including_from, direction
    end

    def commit
      @committed_at = Time.utc
      @database.commit self
      @database
    end
  end
end
