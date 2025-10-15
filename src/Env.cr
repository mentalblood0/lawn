require "yaml"

require "./common"
require "./Transaction"
require "./Log"
require "./SplitDataStorage"
require "./RoundDataStorage"

module Lawn
  class Env
    Lawn.mserializable

    getter log : Log
    getter data_storage : RoundDataStorage

    @[YAML::Field(ignore: true)]
    getter memtable : Hash(Bytes, Bytes?) = Hash(Bytes, Bytes?).new

    getter indexes : {dir: String, pointer_size: UInt8}

    @[YAML::Field(ignore: true)]
    getter indexes_old_to_new = [] of File

    def initialize(@log, @data_storage, @indexes)
    end

    def after_initialize
      @log.read { |kv| @memtable[kv[0]] = kv[1] }
      filenames = Dir.new(@indexes[:dir]).children.select { |filename| filename.starts_with? "index_" }
      filenames.sort!
      @indexes_old_to_new = filenames.map do |filename|
        file = File.new "#{@indexes[:dir]}/#{filename}", "w+"
        file.sync = true
        file
      end
    end

    def get_from_checkpointed(key : Bytes)
      (@indexes_old_to_new.size - 1).downto(0) do |index_index|
        index = @indexes_old_to_new[index_index]
        index_size = index.size // @indexes[:pointer_size]

        data_id : RoundDataStorage::Id? = nil
        current_value : Bytes? = nil
        result_index = (0..index_size - 1).bsearch do |i0|
          ::Log.debug { "Env.get_from_checkpointed #{index.path}[#{i0}]" }
          comparison = false
          (i0..index_size - 1).each do |i|
            ::Log.debug { "Env.get_from_checkpointed i = #{i}" }
            index.pos = i * @indexes[:pointer_size]
            rounded_size_index = Lawn.decode_number(index, 1).not_nil!.to_u8
            pointer = Lawn.decode_number(index, @indexes[:pointer_size]).not_nil!
            data_id = {rounded_size_index, pointer}

            data = IO::Memory.new @data_storage.get(data_id).not_nil! rescue next
            current_value = Lawn.decode_bytes_with_size_size data
            ::Log.debug { "Env.get_from_checkpointed current_value = #{current_value ? current_value.hexstring : nil}" }
            current_key = data.getb_to_end
            ::Log.debug { "Env.get_from_checkpointed current_key = #{current_key.hexstring}" }

            comparison = (key >= current_key)
          end
          comparison
        end
        if result_index
          r = {data_id: data_id.not_nil!, value: current_value} if result_index
          ::Log.debug { "Env.get_from_checkpointed r = #{r}" }
          return r
        end
      end
    end

    def checkpoint
      ::Log.debug { "Env.checkpoint" }
      return self if @memtable.empty?

      sorted_keyvalues = @memtable.to_a
      sorted_keyvalues.sort_by! { |key, _| key }

      to_add = [] of Bytes
      to_delete = [] of RoundDataStorage::Id
      sorted_keyvalues.each do |key, value|
        to_delete << get_from_checkpointed(key).not_nil![:data_id] rescue nil unless value
        value_key_encoded = IO::Memory.new
        Lawn.encode_bytes_with_size_size value_key_encoded, value
        Lawn.encode_bytes value_key_encoded, key
        to_add << value_key_encoded.to_slice
      end
      pointers = @data_storage.update add: to_add, delete: to_delete

      index = File.new "#{@indexes[:dir]}/sorted_headers_pointers_#{(@indexes_old_to_new.size + 1).to_s.rjust 10, '0'}.idx", "w+"
      index.sync = true
      buf = IO::Memory.new
      pointers.each do |rounded_size_index, pointer|
        Lawn.encode_number buf, rounded_size_index, 1
        Lawn.encode_number buf, pointer, @indexes[:pointer_size]
      end
      index.write buf.to_slice
      indexes_old_to_new << index

      @log.truncate
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
