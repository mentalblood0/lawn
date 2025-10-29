require "./exceptions"
require "./Log"

module Lawn
  class Transaction
    EMPTY_VALUE = Bytes.new 0

    getter database : Database

    getter changes : Array(AVLTree)
    getter accessed_keys = {read: Set({UInt8, Key}).new, write: Set({UInt8, Key}).new}
    getter cursors = [] of {UInt8, Cursor(RoundDataStorage::Id) | Cursor(Int64)}
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
          raise Exception.new "Transaction #{self} try to set in table with id #{table_id} value by key #{key}, but value at this key was changed during this transaction by another committed transaction, so this transaction can not perform this get"
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
          raise Exception.new "Transaction #{self} try to set in table with id #{table_id} value by key #{key}, but value at this key was changed during this transaction by another committed transaction, so this transaction can not perform this get"
        end
      end
      @accessed_keys[:read] << {table_id, key}

      result = @changes[table_id][key]?
      return result if !result.is_a? Symbol

      @database.tables[table_id].get key
    end

    class Cursor(I)
      getter table : Table(I)
      getter memtable_cursor : AVLTree::Cursor | Lawn::Cursor
      getter index_cursor : Index::Cursor(I)
      getter memtable_current : {Key, Value?}?
      getter index_current : KeyValue?
      getter from : Key?
      getter direction : Symbol

      getter keyvalue : KeyValue? = nil
      getter last_key_yielded_from_memtable : Key? = nil
      getter range : Range(Key?, Key?)? = nil

      protected def initialize(transaction : Transaction, table_id : UInt8, @from : Key? = nil, including_from : Bool = true, @direction = :forward)
        ::Log.debug { "#{self.class}.initialize from: #{from ? from.hexstring : nil}, including_from: #{including_from}, direction: #{@direction}" }

        @table = transaction.database.tables[table_id].as Table(I)
        @memtable_cursor = Lawn::Cursor.new transaction.changes[table_id], @table.memtable, from, including_from
        case @direction
        when :forward
          index_from = if from_temp = @from
                         if from_found = @table.get_from_checkpointed(from_temp, strict: false, condition: including_from ? :greater_or_equal : :greater)
                           from_found.not_nil![:index_i]
                         else
                           Int64::MAX
                         end
                       else
                         0_i64
                       end
          @memtable_current = @memtable_cursor.next
        when :backward
          index_from = if from_temp = @from
                         if from_found = @table.get_from_checkpointed(from_temp, strict: false, condition: including_from ? :less_or_equal : :less)
                           from_found.not_nil![:index_i]
                         else
                           -1_i64
                         end
                       else
                         @table.index.size - 1
                       end
          @memtable_current = @memtable_cursor.previous
        else raise Exception.new "Unsupported direction: #{@direction}"
        end

        @index_cursor = Index::Cursor.new @table.index, index_from
        @index_current = (index_id = @index_cursor.value) && @table.get_data index_id
      end

      def next : KeyValue?
        ::Log.debug { "#{self.class}.next" }
        case @direction
        when :forward
          loop do
            result = nil
            case {memtable_current_temp = @memtable_current, index_current_temp = @index_current}
            when {Tuple(Key, Value?), nil}
              @last_key_yielded_from_memtable = memtable_current_temp[0]
              result = {memtable_current_temp[0], memtable_current_temp[1].not_nil!} if memtable_current_temp[1]
              @memtable_current = @memtable_cursor.next
            when {Tuple(Key, Value?), KeyValue}
              if memtable_current_temp[0] <= index_current_temp[0]
                @last_key_yielded_from_memtable = memtable_current_temp[0]
                result = {memtable_current_temp[0], memtable_current_temp[1].not_nil!} if memtable_current_temp[1]
                @memtable_current = @memtable_cursor.next
              else
                result = index_current_temp unless index_current_temp[0] == @last_key_yielded_from_memtable
                @index_current = (index_id = @index_cursor.next) && @table.get_data index_id
              end
            when {nil, KeyValue}
              result = index_current_temp unless index_current_temp[0] == @last_key_yielded_from_memtable
              @index_current = (index_id = @index_cursor.next) && @table.get_data index_id
            when {nil, nil}
              break
            end
            if result
              @range = (from..result[0].as(Key?))
              @keyvalue = result
              return result
            end
          end
        when :backward
          loop do
            result = nil
            case {memtable_current_temp = @memtable_current, index_current_temp = @index_current}
            when {Tuple(Key, Value?), nil}
              @last_key_yielded_from_memtable = memtable_current_temp[0]
              result = {memtable_current_temp[0], memtable_current_temp[1].not_nil!} if memtable_current_temp[1]
              @memtable_current = @memtable_cursor.previous
            when {Tuple(Key, Value?), KeyValue}
              if memtable_current_temp[0] >= index_current_temp[0]
                @last_key_yielded_from_memtable = memtable_current_temp[0]
                result = {memtable_current_temp[0], memtable_current_temp[1].not_nil!} if memtable_current_temp[1]
                @memtable_current = @memtable_cursor.previous
              else
                result = index_current_temp unless index_current_temp[0] == @last_key_yielded_from_memtable
                @index_current = (index_id = @index_cursor.previous) && @table.get_data index_id
              end
            when {nil, KeyValue}
              result = index_current_temp unless index_current_temp[0] == @last_key_yielded_from_memtable
              @index_current = (index_id = @index_cursor.previous) && @table.get_data index_id
            when {nil, nil}
              break
            end
            if result
              @range = (result[0].as(Key?)..from)
              @keyvalue = result
              return result
            end
          end
        else raise Exception.new "Unsupported direction: #{@direction}"
        end
      end

      def each_next(&)
        while (next_keyvalue = self.next)
          yield next_keyvalue
        end
      end

      def all_next : Array(KeyValue)
        result = [] of KeyValue
        each_next { |next_keyvalue| result << next_keyvalue }
        result
      end
    end

    def cursor(table_id : UInt8, from : Key? = nil, including_from : Bool = true, direction = :forward)
      result = case table = @database.tables[table_id]
               when VariableTable
                 Cursor(RoundDataStorage::Id).new self, table_id, from, including_from, direction
               when FixedTable
                 Cursor(Int64).new self, table_id, from, including_from, direction
               else
                 raise Exception.new "Unsupported table class #{table.class} for cursor"
               end
      @cursors << {table_id, result}
      result
    end

    def commit
      @committed_at = Time.utc
      @database.commit self
      @database
    end
  end
end
