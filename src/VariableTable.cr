require "yaml"

require "./common"
require "./exceptions"
require "./RoundDataStorage"
require "./Index"
require "./Table"

module Lawn
  class VariableTable < Table(RoundDataStorage::Id)
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

    getter index : Index

    def index=(new_index : Lawn::Index(RoundDataStorage::Id)) : Nil
      case new_index
      when Index then @index = new_index
      else            raise Exception.new "New index should be of the same type as old"
      end
    end

    def data_storage : RoundDataStorage
      @data_storage.as RoundDataStorage
    end

    def initialize(@data_storage, @index)
      after_initialize
    end

    def after_initialize
      raise Exception.new "#{self.class}: Config do not match index schema in #{index.path}, can not operate as may corrupt data" unless !index.bytesize || (index.schema_byte == schema_byte index.pointer_size)
    end

    protected def encode_index_entry(io : IO, element_id : RoundDataStorage::Id, pointer_size : UInt8) : Nil
      io.write_byte element_id[:rounded_size_index]
      Lawn.encode_number io, element_id[:pointer], pointer_size
    end

    protected def encode_keyvalue(keyvalue : KeyValue) : Bytes
      value_key_encoded = IO::Memory.new
      Lawn.encode_bytes_with_size_size value_key_encoded, keyvalue[1]
      value_key_encoded.write keyvalue[0]
      value_key_encoded.to_slice
    end

    protected def decode_keyvalue(data : Bytes) : KeyValue
      data_io = IO::Memory.new data
      value = Lawn.decode_bytes_with_size_size data_io
      key = data_io.getb_to_end
      {key, value}
    end

    protected def pointer_from(element_id : RoundDataStorage::Id) : Int64
      element_id[:pointer]
    end

    protected def schema_byte(pointer_size : UInt8) : UInt8
      pointer_size * 2 + 0
    end
  end
end
