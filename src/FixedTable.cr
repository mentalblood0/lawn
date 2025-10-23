require "yaml"

require "./common"
require "./exceptions"
require "./AlignedList"
require "./Index"
require "./Table"

module Lawn
  class FixedTable < Table(Int64)
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

    def index=(new_index : Lawn::Index(Int64)) : Nil
      case new_index
      when Index then @index = new_index
      else            raise Exception.new "New index should be of the same type as old"
      end
    end

    def data_storage : AlignedList
      @data_storage ||= AlignedList.new data_storage_path, @key_size + @value_size
      @data_storage.as AlignedList
    end

    def initialize(@data_storage_path, @key_size, @value_size, @index)
      after_initialize
    end

    def after_initialize
      raise Exception.new "#{self.class}: Config do not match index schema in #{index.path}, can not operate as may corrupt data" unless (index.bytesize == 0) || (index.schema_byte == schema_byte index.pointer_size)
    end

    protected def encode_index_entry(io : IO, element_id : Int64, pointer_size : UInt8) : Nil
      Lawn.encode_number io, element_id, pointer_size
    end

    protected def encode_keyvalue(keyvalue : KeyValue) : Bytes
      keyvalue[0] + keyvalue[1]
    end

    protected def decode_keyvalue(data : Bytes) : KeyValue
      key = data[..@key_size - 1]
      value = data[@key_size..]
      {key, value}
    end

    protected def pointer_from(element_id : Int64) : Int64
      element_id
    end

    protected def schema_byte(pointer_size : UInt8) : UInt8
      pointer_size * 2 + 1
    end
  end
end
