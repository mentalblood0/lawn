require "yaml"

require "./common"
require "./Transaction"
require "./Log"
require "./SplitDataStorage"
require "./RoundDataStorage"
require "./Index"

module Lawn
  class Env
    Lawn.mserializable

    getter log : Log
    getter data_storage : RoundDataStorage
    getter index : Index

    getter memtable : Hash(Key, Value?) = Hash(Key, Value?).new

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

    protected def get_data(data_id : RoundDataStorage::Id)
      data = IO::Memory.new @data_storage.get(data_id).not_nil!
      value = Lawn.decode_bytes_with_size_size data
      key = data.getb_to_end
      {key, value}
    end

    def dump
      String.build do |s|
        @index.each_with_index do |id, i|
          data = get_data id
          s << "Env.dump #{i.to_s.rjust 3, '0'} #{id} => #{data[0].hexstring} : #{(d = data[1]) ? d.hexstring : nil}\n"
        end
      end
    end

    def get_from_checkpointed(key : Bytes)
      ::Log.debug { "Env.get_from_checkpointed (0..#{@index.size - 1}).bsearch" }

      cache = {} of Int64 => {data_id: RoundDataStorage::Id, keyvalue: KeyValue}
      result_index = (0_i64..@index.size - 1).bsearch do |i|
        ::Log.debug { "Env.get_from_checkpointed i = #{i}" }
        data_id = @index[i]
        current_keyvalue = get_data data_id
        cache[i] = {data_id: data_id, keyvalue: current_keyvalue}
        r = (current_keyvalue[0] >= key)
        ::Log.debug { "Env.get_from_checkpointed (#{current_keyvalue[0].hexstring} >= #{key.hexstring}) == #{r}" }
        r
      end

      if result_index
        result = cache[result_index]
        return nil unless result[:keyvalue][0] == key
        r = {data_id: result[:data_id].not_nil!, value: result[:keyvalue] ? result[:keyvalue][1] : nil}
        ::Log.debug { "Env.get_from_checkpointed => #{r}" }
        r
      end
    end

    def checkpoint
      ::Log.debug { "Env.checkpoint" }
      return self if @memtable.empty?

      sorted_keyvalues = [] of KeyValue
      to_delete = Set(RoundDataStorage::Id).new
      @memtable.each do |key, value|
        if value
          sorted_keyvalues << {key, value}
        else
          to_delete << get_from_checkpointed(key).not_nil![:data_id] rescue nil
        end
      end
      sorted_keyvalues.sort_by! { |key, _| key }

      new_index_ids = begin
        to_add = [] of Bytes
        sorted_keyvalues.each do |key, value|
          value_key_encoded = IO::Memory.new
          Lawn.encode_bytes_with_size_size value_key_encoded, value
          Lawn.encode_bytes value_key_encoded, key
          to_add << value_key_encoded.to_slice
        end
        @data_storage.update add: to_add, delete: to_delete.to_a
      end

      new_index_file = File.new "#{@index.file.path}.new", "w"
      new_index_file.sync = true
      new_i = 0
      @index.each do |old_index_id|
        next if to_delete.includes? old_index_id
        old_index_keyvalue = get_data old_index_id
        while (new_i < new_index_ids.size) && begin
                new_index_keyvalue = sorted_keyvalues[new_i]
                new_index_keyvalue[0] <= old_index_keyvalue[0]
              end
          new_index_id = new_index_ids[new_i]
          ::Log.debug { "Env.checkpoint write #{new_index_id} #{sorted_keyvalues[new_i][0].hexstring}" }
          Lawn.encode_number new_index_file, new_index_id[:rounded_size_index], 1
          Lawn.encode_number new_index_file, new_index_id[:pointer], @index.pointer_size
          new_i += 1
        end
        ::Log.debug { "Env.checkpoint write #{old_index_id} #{old_index_keyvalue[0].hexstring}" }
        Lawn.encode_number new_index_file, old_index_id[:rounded_size_index], 1
        Lawn.encode_number new_index_file, old_index_id[:pointer], @index.pointer_size
      end
      while new_i < new_index_ids.size
        new_index_keyvalue = sorted_keyvalues[new_i]
        new_index_id = new_index_ids[new_i]
        ::Log.debug { "Env.checkpoint write #{new_index_id} #{new_index_keyvalue[0].hexstring}" }
        Lawn.encode_number new_index_file, new_index_id[:rounded_size_index], 1
        Lawn.encode_number new_index_file, new_index_id[:pointer], @index.pointer_size
        new_i += 1
      end
      new_index_file.rename @index.file.path
      new_index_file.close
      @index = Index.new Path.new(new_index_file.path), @index.pointer_size

      @log.clear
      @memtable.clear
      ::Log.debug { "\n\n#{dump}" }
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
