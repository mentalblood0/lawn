require "yaml"

require "./exceptions"
require "./common"
require "./Log"
require "./AVLTree"
require "./Index"

module Lawn
  abstract class Table(I)
    Lawn.mserializable

    abstract def index : Index(I)
    abstract def index=(new_index : Index(I)) : Nil

    @data_storage : (AlignedList | RoundDataStorage)?

    abstract def data_storage : DataStorage(I)

    Lawn.mignore
    getter memtable = AVLTree.new

    def clear
      data_storage.clear
      index.clear
      memtable.clear
    end

    def bytesize_disk
      data_storage.bytesize_disk + index.bytesize
    end

    protected def get_data(data_id : I) : KeyValue
      decode_keyvalue data_storage.get(data_id).not_nil!
    end

    protected def get_from_checkpointed(key : Key, strict : Bool = true, condition : Symbol = :greater_or_equal) : {index_i: Int64, data_id: I, value: Value}?
      ::Log.debug { "#{self.class}.get_from_checkpointed key: #{key.hexstring}, strict: #{strict}, condition: #{condition}" }
      return unless index.size > 0

      cache = [] of {i: Int64, result: {data_id: I, keyvalue: KeyValue}}
      result_index = (0_i64..index.size - 1).bsearch do |i|
        i = case condition
            when :greater_or_equal, :greater then i
            when :less_or_equal, :less       then index.size - 1 - i
            else                                  raise Exception.new "Unsupported condition: #{condition}"
            end
        data_id = index[i]
        current_keyvalue = get_data data_id
        cache << {i: i, result: {data_id: data_id, keyvalue: current_keyvalue}}
        case condition
        when :greater_or_equal then current_keyvalue[0] >= key
        when :less_or_equal    then current_keyvalue[0] <= key
        when :less             then current_keyvalue[0] < key
        when :greater          then current_keyvalue[0] > key
        else                        raise Exception.new "Unsupported condition: #{condition}"
        end
      end

      if result_index
        result_index = case condition
                       when :greater_or_equal, :greater then result_index
                       when :less_or_equal, :less       then index.size - 1 - result_index
                       else                                  raise Exception.new "Unsupported condition: #{condition}"
                       end
        cached = cache.find! { |c| c[:i] == result_index }
        return nil if strict && (cached[:result][:keyvalue][0] != key)
        result = {index_i: cached[:i], data_id: cached[:result][:data_id], value: cached[:result][:keyvalue][1]}
        return result
      end
    end

    protected abstract def encode_index_entry(io : IO, element_id : I, pointer_size : UInt8) : Nil
    protected abstract def encode_keyvalue(keyvalue : KeyValue) : Bytes
    protected abstract def decode_keyvalue(data : Bytes) : KeyValue
    protected abstract def pointer_from(element_id : I) : Int64
    protected abstract def schema_byte(pointer_size : UInt8) : UInt8

    def checkpoint
      ::Log.debug { "#{self.class}.checkpoint" }
      return self if @memtable.empty?

      global_i = 0_i64
      new_index_positions = [] of Int64
      new_index_pointer_size = 1_u8

      data_to_add = Array(Bytes).new
      ids_to_delete = Set(I).new

      index_current = nil
      memtable_cursor = AVLTree::Cursor.new @memtable.root
      memtable_current = memtable_cursor.next
      last_key_yielded_from_memtable = nil

      index_cursor = Index::Cursor.new index
      while index_id = index_cursor.value
        index_current_data = get_data index_id
        while memtable_current && (memtable_current[0] <= index_current_data[0])
          last_key_yielded_from_memtable = memtable_current[0]
          if memtable_current[1]
            data_to_add << encode_keyvalue({memtable_current[0], memtable_current[1].not_nil!})
            new_index_positions << global_i
            global_i += 1
          else
          end
          memtable_current = memtable_cursor.next
        end
        if index_current_data[0] == last_key_yielded_from_memtable
          ids_to_delete << index_id
        else
          index_id_pointer_size = Lawn.number_size pointer_from index_id
          new_index_pointer_size = Math.max new_index_pointer_size, index_id_pointer_size
          global_i += 1
        end
        index_cursor.next
      end
      while memtable_current
        if memtable_current[1]
          data_to_add << encode_keyvalue({memtable_current[0], memtable_current[1].not_nil!})
          new_index_positions << global_i
          global_i += 1
        else
        end
        memtable_current = memtable_cursor.next
      end

      new_index_ids = data_storage.update add: data_to_add, delete: ids_to_delete.to_a
      unless new_index_ids.empty?
        new_index_pointer_size = Math.max new_index_pointer_size, new_index_ids.max_of { |index_id| Lawn.number_size pointer_from index_id }
      end

      new_index_file = File.new "#{index.file.path}.new", "w"
      new_index_file.sync = true
      new_index_file.write_byte schema_byte new_index_pointer_size
      global_i = 0_i64
      new_index_ids_i = 0
      index.file.pos = 1
      new_index_positions.each do |new_index_position|
        while global_i < new_index_position
          old_index_id = index.read
          unless ids_to_delete.includes? old_index_id
            encode_index_entry new_index_file, old_index_id, new_index_pointer_size
            global_i += 1
          end
        end
        new_index_id = new_index_ids[new_index_ids_i]
        encode_index_entry new_index_file, new_index_id, new_index_pointer_size
        new_index_ids_i += 1
        global_i += 1
      end
      while ((old_index_id = index.read) rescue nil)
        unless ids_to_delete.includes? old_index_id
          encode_index_entry new_index_file, old_index_id, new_index_pointer_size
        end
      end

      new_index_file.rename index.file.path
      new_index_file.close

      @memtable.clear
      self.index = self.index.class.new Path.new(new_index_file.path), index.cache_size

      self
    end

    class Cursor(I)
      getter table : Table(I)
      getter memtable_cursor : AVLTree::Cursor
      getter index_cursor : Index::Cursor(I)
      getter memtable_current : {Key, Value?}?
      getter index_current : KeyValue?
      getter last_key_yielded_from_memtable : Key? = nil
      getter keyvalue : KeyValue? = nil
      getter direction : Symbol

      def initialize(@table, from : Key? = nil, including_from : Bool = true, @direction = :forward)
        ::Log.debug { "#{self.class}.initialize from: #{from ? from.hexstring : nil}, including_from: #{including_from}, direction: #{@direction}" }

        @memtable_cursor = AVLTree::Cursor.new @table.memtable.root, from, including_from
        case @direction
        when :forward
          index_from = if from
                         if from_found = @table.get_from_checkpointed(from, strict: false, condition: including_from ? :greater_or_equal : :greater)
                           from_found.not_nil![:index_i]
                         else
                           Int64::MAX
                         end
                       else
                         0_i64
                       end
          @memtable_current = @memtable_cursor.next
        when :backward
          index_from = if from
                         if from_found = @table.get_from_checkpointed(from, strict: false, condition: including_from ? :less_or_equal : :less)
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

    def cursor(from : Key? = nil, including_from : Bool = true, direction = :forward)
      Cursor(I).new self, from, including_from, direction
    end

    def get(key : Key) : Value?
      ::Log.debug { "#{self.class}.get #{key.hexstring}" }

      result = @memtable[key]?
      return result if result

      result = get_from_checkpointed key
      return result[:value] if result
    end
  end
end
