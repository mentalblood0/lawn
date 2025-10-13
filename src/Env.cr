require "yaml"

require "./common"
require "./Transaction"
require "./Log"
require "./SplitDataStorage"
require "./SortedPointers"

module Lawn
  class Env
    Lawn.mserializable

    getter key_size_size : UInt8

    getter log : Log
    getter split_data_storage : SplitDataStorage

    @[YAML::Field(ignore: true)]
    getter memtable : Hash(Bytes, Bytes?) = Hash(Bytes, Bytes?).new

    getter sorted_pointers_dir : String

    @[YAML::Field(ignore: true)]
    getter sorted_pointers_old_to_new = [] of SortedPointers

    def initialize(@key_size_size, @log, @split_data_storage, @sorted_pointers_dir)
    end

    def after_initialize
      @log.read { |kv| @memtable[kv[0]] = kv[1] }
      filenames = Dir.glob(sorted_pointers_dir).children.select { |filename| filename.starts_with "sorted_pointers_" }
      filenames.sort!
      @sorted_pointers_old_to_new = filenames.map { |filename| SortedPointers.new File.new Path.new(sorted_pointers_dir) / filename, "w+" }
    end

    def get_header_pointer(key : Bytes)
      (@sorted_pointers_old_to_new.size - 1).downto(0) do |i|
        r = @sorted_pointers_old_to_new[i].get(key) { |k| @split_data_storage.get k }
        return r if r
      end
    end

    def checkpoint
      return self if @memtable.empty?

      sorted_keyvalues = @memtable.to_a
      sorted_keyvalues.sort_by! { |key, _| key }

      to_add = [] of Bytes
      to_delete = [] of UInt64
      sorted_keyvalues.each do |key, value|
        case value
        when nil
          to_delete << get_header_pointer key
        else
          key_value_encoded = IO::Memory.new
          Lawn.encode_bytes_with_size key_value_encoded, key, @key_size_size
          Lawn.encode_bytes key_value_encoded, value
          to_add << key_value_encoded.to_slice
        end
      end
      pointers = @split_data_storage.update add: to_add, delete: to_delete

      sorted_pointers = SortedPointers.new Path.new(@sorted_pointers_dir) / "sorted_headers_pointers_#{(@sorted_pointers_old_to_new.size + 1).to_s.rjust 10, '0'}.idx"
      sorted_pointers.write pointers

      @log.truncate
      @memtable.clear
      self
    end

    def transaction
      Transaction.new self
    end

    def get(k : Bytes)
      return @memtable[k]?
    end
  end
end
