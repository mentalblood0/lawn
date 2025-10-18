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

    def get_from_checkpointed(key : Bytes) : {data_id: RoundDataStorage::Id, value: Value}?
      ::Log.debug { "Env.get_from_checkpointed (0..#{@index.size - 1}).bsearch" }

      cache = [] of {i: Int64, result: {data_id: RoundDataStorage::Id, keyvalue: KeyValue}}
      result_index = (0_i64..@index.size - 1).bsearch do |i|
        ::Log.debug { "Env.get_from_checkpointed i = #{i}" }
        data_id = @index[i]
        current_keyvalue = get_data data_id
        cache << {i: i, result: {data_id: data_id, keyvalue: current_keyvalue}}
        current_keyvalue[0] >= key
      end

      if result_index
        result = cache.find! { |c| c[:i] == result_index }[:result]
        return nil unless result[:keyvalue][0] == key
        {data_id: result[:data_id], value: result[:keyvalue][1]}
      end
    end

    def checkpoint
      ::Log.debug { "Env.checkpoint" }
      return self if @memtable.empty?

      keys = [] of Key
      to_delete = Set(RoundDataStorage::Id).new
      new_index_ids = begin
        to_add = [] of Bytes
        @memtable.each do |key, value|
          if value
            keys << key
            value_key_encoded = IO::Memory.new
            Lawn.encode_bytes_with_size_size value_key_encoded, value
            value_key_encoded.write key
            to_add << value_key_encoded.to_slice
          else
            to_delete << get_from_checkpointed(key).not_nil![:data_id] rescue nil
          end
        end
        @data_storage.update add: to_add, delete: to_delete.to_a
      end

      new_index_file = File.new "#{@index.file.path}.new", "w"
      new_index_file.sync = true
      new_i = 0
      @index.each do |old_index_id|
        next if to_delete.includes? old_index_id
        old_index_key = nil
        overwrite_old_index_record = false
        loop do
          break unless new_i < new_index_ids.size
          old_index_key = get_data(old_index_id)[0] unless old_index_key
          break unless keys[new_i] <= old_index_key.not_nil!

          overwrite_old_index_record = overwrite_old_index_record || (keys[new_i] == old_index_key.not_nil!)

          new_index_id = new_index_ids[new_i]
          new_index_file.write_byte new_index_id[:rounded_size_index]
          Lawn.encode_number new_index_file, new_index_id[:pointer], @index.pointer_size
          new_i += 1
        end
        next if overwrite_old_index_record
        new_index_file.write_byte old_index_id[:rounded_size_index]
        Lawn.encode_number new_index_file, old_index_id[:pointer], @index.pointer_size
      end
      while new_i < new_index_ids.size
        new_index_id = new_index_ids[new_i]
        new_index_file.write_byte new_index_id[:rounded_size_index]
        Lawn.encode_number new_index_file, new_index_id[:pointer], @index.pointer_size
        new_i += 1
      end
      new_index_file.rename @index.file.path
      new_index_file.close
      @index = Index.new Path.new(new_index_file.path), @index.pointer_size

      @log.clear
      @memtable.clear
      self
    end

    def each(& : KeyValue ->)
      memtable_cursor = AVLTree::Cursor.new @memtable.root
      index_current = nil
      memtable_current = nil
      last_key_yielded_from_memtable = nil
      @index.each do |index_id|
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

    def each
      r = [] of KeyValue
      each do |keyvalue|
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
