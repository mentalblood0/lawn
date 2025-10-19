require "yaml"

require "./common"
require "./Transaction"
require "./Log"
require "./SplitDataStorage"
require "./RoundDataStorage"
require "./Index"
require "./AVLTree"

module Lawn
  class Env
    Lawn.mserializable

    getter log : Log
    getter data_storage : RoundDataStorage
    getter index : Index

    Lawn.mignore
    getter memtable = AVLTree.new

    def initialize(@log, @data_storage, @index)
    end

    def after_initialize
      @log.read { |kv| @memtable[kv[0]] = kv[1] }
    end

    def clear
      log.clear
      data_storage.clear
      index.clear
      after_initialize
    end

    protected def get_data(data_id : RoundDataStorage::Id) : KeyValue
      data = IO::Memory.new @data_storage.get(data_id).not_nil!
      value = Lawn.decode_bytes_with_size_size data
      key = data.getb_to_end
      {key, value}
    end

    def get_from_checkpointed(key : Bytes, strict : Bool = true) : {index_i: Int64, data_id: RoundDataStorage::Id, value: Value}?
      ::Log.debug { "Env.get_from_checkpointed #{key.hexstring} while @index.size = #{@index.size}" }
      return unless @index.size > 0

      cache = [] of {i: Int64, result: {data_id: RoundDataStorage::Id, keyvalue: KeyValue}}
      result_index = (0_i64..@index.size - 1).bsearch do |i|
        data_id = @index[i]
        current_keyvalue = get_data data_id
        cache << {i: i, result: {data_id: data_id, keyvalue: current_keyvalue}}
        current_keyvalue[0] >= key
      end

      if result_index
        cached = cache.find! { |c| c[:i] == result_index }
        return nil if strict && (cached[:result][:keyvalue][0] != key)
        {index_i: cached[:i], data_id: cached[:result][:data_id], value: cached[:result][:keyvalue][1]}
      end
    end

    protected def encode(keyvalue : KeyValue) : Bytes
      value_key_encoded = IO::Memory.new
      Lawn.encode_bytes_with_size_size value_key_encoded, keyvalue[1]
      value_key_encoded.write keyvalue[0]
      value_key_encoded.to_slice
    end

    def checkpoint
      ::Log.debug { "Env.checkpoint" }
      return self if @memtable.empty?

      global_i = 0_i64
      new_index_positions = [] of Int64
      new_index_pointer_size = 1_u8

      data_to_add = Array(Bytes).new
      ids_to_delete = Set(RoundDataStorage::Id).new

      index_current = nil
      memtable_cursor = AVLTree::Cursor.new @memtable.root
      memtable_current = memtable_cursor.next
      last_key_yielded_from_memtable = nil

      @index.each do |index_id|
        index_current = get_data index_id
        while memtable_current && (memtable_current[0] <= index_current[0])
          last_key_yielded_from_memtable = memtable_current[0]
          if memtable_current[1]
            data_to_add << encode({memtable_current[0], memtable_current[1].not_nil!})
            new_index_positions << global_i
            global_i += 1
          else
          end
          memtable_current = memtable_cursor.next
        end
        if index_current[0] == last_key_yielded_from_memtable
          ids_to_delete << index_id
        else
          index_id_pointer_size = Lawn.number_size index_id[:pointer]
          new_index_pointer_size = Math.max new_index_pointer_size, index_id_pointer_size
          global_i += 1
        end
      end
      while memtable_current
        if memtable_current[1]
          data_to_add << encode({memtable_current[0], memtable_current[1].not_nil!})
          new_index_positions << global_i
          global_i += 1
        else
        end
        memtable_current = memtable_cursor.next
      end

      new_index_ids = @data_storage.update add: data_to_add, delete: ids_to_delete.to_a
      unless new_index_ids.empty?
        new_index_pointer_size = Math.max new_index_pointer_size, new_index_ids.max_of { |index_id| Lawn.number_size index_id[:pointer] }
      end

      new_index_file = File.new "#{@index.file.path}.new", "w"
      new_index_file.sync = true
      new_index_file.write_byte new_index_pointer_size
      global_i = 0_i64
      new_index_ids_i = 0
      @index.file.pos = 1
      new_index_positions.each do |new_index_position|
        while global_i < new_index_position
          old_index_id = @index.read
          unless ids_to_delete.includes? old_index_id
            new_index_file.write_byte old_index_id[:rounded_size_index]
            Lawn.encode_number new_index_file, old_index_id[:pointer], new_index_pointer_size
            global_i += 1
          end
        end
        new_index_id = new_index_ids[new_index_ids_i]
        new_index_file.write_byte new_index_id[:rounded_size_index]
        Lawn.encode_number new_index_file, new_index_id[:pointer], new_index_pointer_size
        new_index_ids_i += 1
        global_i += 1
      end
      while ((old_index_id = @index.read) rescue nil)
        unless ids_to_delete.includes? old_index_id
          new_index_file.write_byte old_index_id[:rounded_size_index]
          Lawn.encode_number new_index_file, old_index_id[:pointer], new_index_pointer_size
        end
      end

      new_index_file.rename @index.file.path
      new_index_file.close
      @index = Index.new Path.new(new_index_file.path)

      @log.clear
      @memtable.clear
      self
    end

    def each(from : Key? = nil, & : KeyValue ->)
      memtable_cursor = AVLTree::Cursor.new @memtable.root
      index_current = nil
      memtable_current = nil
      last_key_yielded_from_memtable = nil

      index_from = from ? (get_from_checkpointed(from, strict: false).not_nil![:index_i] rescue 0_i64) : 0_i64

      @index.each(index_from) do |index_id|
        index_current = get_data(index_id).not_nil!
        while (memtable_current = memtable_cursor.next) && (memtable_current[0] <= index_current[0])
          if memtable_current.is_a? KeyValue
            last_key_yielded_from_memtable = memtable_current[0]
            yield memtable_current
          end
        end
        yield index_current unless index_current[0] == last_key_yielded_from_memtable
      end
      while memtable_current
        yield memtable_current if memtable_current.is_a? KeyValue
        memtable_current = memtable_cursor.next
      end
    end

    def each(from : Key? = nil)
      r = [] of KeyValue
      each(from) do |keyvalue|
        r << keyvalue
      end
      r
    end

    def transaction
      ::Log.debug { "Env.transaction" }
      Transaction.new self
    end

    def get(key : Bytes) : Value?
      ::Log.debug { "Env.get #{key.hexstring}" }

      r = @memtable[key]?
      return r if r

      r = get_from_checkpointed key
      return r[:value] if r
    end
  end
end
