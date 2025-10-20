require "json"
require "yaml"

require "./common"

module Lawn
  class Log
    Lawn.mserializable

    getter path : Path

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
    end

    def clear
      file.truncate
    end

    def write(tables : Array(Table | FixedTable), batches : Array(Array({Key, Value?})?))
      buf = IO::Memory.new
      batches.each_with_index do |batch, table_id|
        next unless batch
        buf.write_byte table_id.to_u8
        Lawn.encode_number_with_size buf, batch.size
        if (table = tables[table_id]).is_a?(FixedTable)
          batch.each do |key, value|
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
          batch.each do |key, value|
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
    end

    def read(tables : Array(Table | FixedTable), &)
      file.pos = 0
      loop do
        table_id = file.read_byte.not_nil! rescue break
        batch_size = Lawn.decode_number_with_size file
        if (table = tables[table_id]).is_a?(FixedTable)
          batch_size.times do
            key = Lawn.decode_bytes file, table.key_size
            value = case file.read_byte.not_nil!
                    when 1_u8 then Lawn.decode_bytes file, table.value_size
                    when 0_u8 then nil
                    end
            yield({table_id: table_id, keyvalue: {key, value}})
          end
        else
          batch_size.times do
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
