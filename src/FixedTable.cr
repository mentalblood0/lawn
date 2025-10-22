require "yaml"

require "./common"
require "./checkpoint"
require "./Transaction"
require "./Log"
require "./AlignedList"
require "./AVLTree"
require "./Index"

module Lawn
  class FixedTable
    Lawn.mserializable

    class Index < Index(Int64)
      Lawn.mserializable

      def initialize(@path, @cache_size)
      end

      def element_size : UInt8
        pointer_size
      end

      def read(source : IO = file) : Int64
        Lawn.decode_number(source, pointer_size).not_nil!
      end
    end

    getter data_storage_path : Path
    getter key_size : Int32
    getter value_size : Int32
    getter index : Index

    Lawn.mignore
    getter data_storage : AlignedList { AlignedList.new data_storage_path, @key_size + @value_size }

    Lawn.mignore
    getter memtable = AVLTree.new

    def initialize(@data_storage_path, @key_size, @value_size, @index)
    end

    def clear
      data_storage.clear
      index.clear
    end

    protected def get_data(data_id : Int64) : KeyValue
      data = data_storage.get(data_id).not_nil!
      key = data[..@key_size - 1]
      value = data[@key_size..]
      {key, value}
    end

    def get_from_checkpointed(key : Bytes, strict : Bool = true) : {index_i: Int64, data_id: Int64, value: Value}?
      ::Log.debug { "Env.get_from_checkpointed #{key.hexstring} while @index.size = #{@index.size}" }
      return unless @index.size > 0

      cache = [] of {i: Int64, result: {data_id: Int64, keyvalue: KeyValue}}
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
      keyvalue[0] + keyvalue[1]
    end

    protected def encode(io : IO, id : Int64, pointer_size : UInt8)
      Lawn.encode_number io, id, pointer_size
    end

    Lawn.def_checkpoint(Int64, nil)

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
