require "json"
require "yaml"

require "./common"

module Lawn
  class Log
    Lawn.serializable

    getter path : Path

    Lawn.mignore
    getter bytesize : Int64 = 0_i64

    Lawn.mignore
    getter file : File do
      unless File.exists? @path
        Dir.mkdir_p @path.parent
        File.touch @path
      end
      r = File.new @path, "a+"
      r.sync = true
      r
    end

    def initialize(@path)
      after_initialize
    end

    def after_initialize
      @bytesize = file.size
    end

    def clear
      file.truncate
      after_initialize
    end

    def write(tables : Array(Table), changes : Array(AVLTree))
      buf = IO::Memory.new
      changes.each_with_index do |table_changes, table_id|
        next if table_changes.empty?
        buf.write_byte table_id.to_u8
        Lawn.encode_number_with_size buf, table_changes.size
        if (table = tables[table_id]).is_a? FixedTable
          table_changes.cursor.each_next do |key, value|
            raise Exception.new "Key size must be #{table.key_size}, not #{key.size}" unless key.size == table.key_size
            raise Exception.new "Value size must be #{table.value_size}, not #{value.size}" unless !value || (value.size == table.value_size)
            buf.write key
            if value
              buf.write_byte 1_u8
              buf.write value
            else
              buf.write_byte 0_u8
            end
          end
        else
          table_changes.cursor.each_next do |key, value|
            Lawn.encode_bytes_with_size_size buf, key
            if value
              buf.write_byte 1_u8
              Lawn.encode_bytes_with_size_size file, value
            else
              buf.write_byte 0_u8
            end
          end
        end
      end
      file.write buf.to_slice
      @bytesize += buf.size
    end

    def read(tables : Array(Table), &)
      file.rewind
      loop do
        table_id = file.read_byte.not_nil! rescue break
        table_changes_count = Lawn.decode_number_with_size file
        if (table = tables[table_id]).is_a? FixedTable
          table_changes_count.times do
            key = Lawn.decode_bytes file, table.key_size
            value = case file.read_byte.not_nil!
                    when 1_u8 then Lawn.decode_bytes file, table.value_size
                    when 0_u8 then nil
                    end
            yield({table_id: table_id, keyvalue: {key, value}})
          end
        else
          table_changes_count.times do
            key = Lawn.decode_bytes_with_size_size file
            value = case file.read_byte.not_nil!
                    when 1_u8 then Lawn.decode_bytes_with_size_size file
                    when 0_u8 then nil
                    end
            yield({table_id: table_id, keyvalue: {key, value}})
          end
        end
      end
    end
  end
end
