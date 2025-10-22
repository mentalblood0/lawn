require "yaml"

require "./common"
require "./checkpoint"
require "./Transaction"
require "./Log"
require "./RoundDataStorage"
require "./AVLTree"
require "./Index"

module Lawn
  class Table
    Lawn.mserializable

    class Index < Index(RoundDataStorage::Id)
      Lawn.mserializable

      def initialize(@path, @cache_size)
      end

      def element_size : UInt8
        pointer_size + 1
      end

      def read(source : IO = file) : RoundDataStorage::Id
        rounded_size_index = source.read_byte.not_nil!
        pointer = Lawn.decode_number(source, pointer_size).not_nil!
        {rounded_size_index: rounded_size_index, pointer: pointer}
      end
    end

    getter data_storage : RoundDataStorage
    getter index : Index

    Lawn.mignore
    getter memtable = AVLTree.new

    def initialize(@data_storage, @index)
    end

    def clear
      data_storage.clear
      index.clear
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

    protected def encode(io : IO, id : RoundDataStorage::Id, pointer_size : UInt8)
      io.write_byte id[:rounded_size_index]
      Lawn.encode_number io, id[:pointer], pointer_size
    end

    Lawn.def_checkpoint(RoundDataStorage::Id, pointer)

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

    def get(key : Bytes) : Value?
      ::Log.debug { "Env.get #{key.hexstring}" }

      r = @memtable[key]?
      return r if r

      r = get_from_checkpointed key
      return r[:value] if r
    end
  end
end
