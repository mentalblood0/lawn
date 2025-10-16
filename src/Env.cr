require "yaml"

require "./common"
require "./Transaction"
require "./Log"
require "./SplitDataStorage"
require "./RoundDataStorage"

module Lawn
  class Index
    Lawn.mserializable

    @[YAML::Field(converter: Lawn::FileConverter)]
    getter file : File

    getter pointer_size : UInt8

    @[YAML::Field(ignore: true)]
    getter size : Int64 = 0_i64

    getter id_size : UInt8 { @pointer_size + 1 }

    def initialize(@file, @pointer_size)
      after_initialize
    end

    def after_initialize
      @size = @file.size // id_size
    end

    def [](i : Int64) : RoundDataStorage::Id
      @file.pos = i * id_size
      rounded_size_index = Lawn.decode_number(@file, 1).not_nil!.to_u8
      pointer = Lawn.decode_number(@file, id_size).not_nil!
      {rounded_size_index, pointer}
    end
  end

  class Env
    Lawn.mserializable

    getter log : Log
    getter data_storage : RoundDataStorage
    getter index : Index

    @[YAML::Field(ignore: true)]
    getter memtable : Hash(Bytes, Bytes?) = Hash(Bytes, Bytes?).new

    def initialize(@log, @data_storage, @index)
    end

    def after_initialize
      @log.read { |kv| @memtable[kv[0]] = kv[1] }
    end

    def get_from_checkpointed(key : Bytes)
      data_id : RoundDataStorage::Id? = nil
      current_value : Bytes? = nil
      result_index = (0_i64..@index.size - 1).bsearch do |i|
        data_id = @index[i]

        data = IO::Memory.new @data_storage.get(data_id).not_nil! rescue next
        current_value = Lawn.decode_bytes_with_size_size data
        current_key = data.getb_to_end

        key >= current_key
      end

      if result_index
        r = {data_id: data_id.not_nil!, value: current_value}
        ::Log.debug { "Env.get_from_checkpointed #{key} => #{r}" }
        r
      end
    end

    def checkpoint
      ::Log.debug { "Env.checkpoint" }
      return self if @memtable.empty?

      sorted_keyvalues = @memtable.to_a
      sorted_keyvalues.sort_by! { |key, _| key }

      new_pointers = begin
        to_add = [] of Bytes
        to_delete = [] of RoundDataStorage::Id
        sorted_keyvalues.each do |key, value|
          to_delete << get_from_checkpointed(key).not_nil![:data_id] rescue nil unless value
          value_key_encoded = IO::Memory.new
          Lawn.encode_bytes_with_size_size value_key_encoded, value
          Lawn.encode_bytes value_key_encoded, key
          to_add << value_key_encoded.to_slice
        end
        @data_storage.update add: to_add, delete: to_delete
      end

      new_index_file = File.new "#{@index.file.path}.new", "w+"
      new_index_file.sync = true
      buf = IO::Memory.new
      new_pointers.each do |rounded_size_index, pointer|
        Lawn.encode_number buf, rounded_size_index, 1
        Lawn.encode_number buf, pointer, @index.pointer_size
      end
      new_index_file.write buf.to_slice
      new_index_file.rename @index.file.path
      @index = Index.new new_index_file, @index.id_size

      @log.clear
      @memtable.clear
      self
    end

    def transaction
      ::Log.debug { "Env.transaction" }
      Transaction.new self
    end

    def get(key : Bytes)
      ::Log.debug { "Env.get #{key.hexstring}" }

      r = @memtable[key]?
      return r if r

      r = get_from_checkpointed key
      return r[:value] if r
    end
  end
end
