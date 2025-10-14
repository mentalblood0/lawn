require "yaml"

require "./common"
require "./Transaction"
require "./Log"
require "./SplitDataStorage"
require "./SortedPointers"
require "./RoundDataStorage"

module Lawn
  class Env
    Lawn.mserializable

    getter log : Log
    getter data_storage : RoundDataStorage

    @[YAML::Field(ignore: true)]
    getter memtable : Hash(Bytes, Bytes?) = Hash(Bytes, Bytes?).new

    getter sorted_pointers_dir : String

    @[YAML::Field(ignore: true)]
    getter sorted_pointers_old_to_new = [] of SortedPointers

    def initialize(@log, @data_storage, @sorted_pointers_dir)
    end

    def after_initialize
      @log.read { |kv| @memtable[kv[0]] = kv[1] }
      filenames = Dir.new(sorted_pointers_dir).children.select { |filename| filename.starts_with? "sorted_pointers_" }
      filenames.sort!
      @sorted_pointers_old_to_new = filenames.map do |filename|
        io = File.new "#{sorted_pointers_dir}/#{filename}", "w+"
        io.sync = true
        SortedPointers.new @data_storage.pointer_size, io
      end
    end

    def get_from_checkpointed(key : Bytes)
      (@sorted_pointers_old_to_new.size - 1).downto(0) do |i|
        r = @sorted_pointers_old_to_new[i].get key, ->(header_pointer : UInt64) { @data_storage.get(header_pointer).not_nil! }, @data_storage.data_size_size
        return r if r
      end
    end

    def checkpoint
      ::Log.debug { "Env.checkpoint" }
      return self if @memtable.empty?

      sorted_keyvalues = @memtable.to_a
      sorted_keyvalues.sort! { |a, b| b[0] <=> a[0] }

      to_add = [] of Bytes
      to_delete = [] of UInt64
      sorted_keyvalues.each do |key, value|
        to_delete << get_from_checkpointed(key).not_nil![:header_pointer] rescue nil unless value
        value_key_encoded = IO::Memory.new
        Lawn.encode_bytes_with_size value_key_encoded, value, @data_storage.data_size_size
        Lawn.encode_bytes value_key_encoded, key
        to_add << value_key_encoded.to_slice
      end
      pointers = @data_storage.update add: to_add, delete: to_delete

      io = File.new "#{@sorted_pointers_dir}/sorted_headers_pointers_#{(@sorted_pointers_old_to_new.size + 1).to_s.rjust 10, '0'}.idx", "w+"
      io.sync = true
      sorted_pointers = SortedPointers.new @data_storage.pointer_size, io
      sorted_pointers.write pointers
      sorted_pointers_old_to_new << sorted_pointers

      @log.truncate
      @memtable.clear
      self
    end

    def transaction
      ::Log.debug { "Env.transaction" }
      Transaction.new self
    end

    def get(k : Bytes)
      ::Log.debug { "Env.get #{k.hexstring}" }
      return @memtable[k]?.not_nil! rescue get_from_checkpointed(k).not_nil![:value] rescue nil
    end
  end
end
