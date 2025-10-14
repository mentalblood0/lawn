require "yaml"
require "json"

require "./common"
require "./AlignedList"

module Lawn
  class RoundDataStorage
    Lawn.mserializable

    getter data_size_size : UInt8
    getter pointer_size : UInt8
    getter data_dir : String

    @[YAML::Field(ignore: true)]
    getter data_aligned_lists_by_size_exponent : Array(AlignedList?) = Array(AlignedList?).new 32 { nil }

    def data_aligned_list(size_exponent : UInt8)
      unless data_aligned_lists_by_size_exponent[size_exponent]
        io = File.new (Path.new @data_dir) / "size_and_data_of_size_with_rounded_up_exponent_#{size_exponent.to_s.rjust 2, '0'}.dat", "w+"
        io.sync = true
        data_aligned_lists_by_size_exponent[size_exponent] = AlignedList.new io, (1_u32 << size_exponent)
      end
      data_aligned_lists_by_size_exponent[size_exponent].not_nil!
    end

    def initialize(@data_size_size, @pointer_size, @data_dir)
    end

    alias Id = {UInt8, UInt64}

    alias Add = {data: Bytes, data_index: Int32}

    def update(add : Array(Bytes), delete : Array(Id)) : Array(Id)
      ::Log.debug { "RoundDataStorage.update add: #{add.map &.hexstring}, delete: #{delete}" }

      add_data_by_exponent = Array(Array(Add)?).new(32) { nil }
      add.each_with_index do |data, data_index|
        size_and_data_encoded = IO::Memory.new
        Lawn.encode_bytes_with_size size_and_data_encoded, data, @data_size_size
        e = round_exponent size_and_data_encoded.size
        add_data_by_exponent[e] = Array(Add).new unless add_data_by_exponent[e]
        add_data_by_exponent[e].not_nil! << {data: size_and_data_encoded.to_slice, data_index: data_index}
      end

      delete_pointers_by_exponent = Array(Array(UInt64)?).new(32) { nil }
      delete.each do |e, pointer|
        delete_pointers_by_exponent[e] = Array(UInt64).new unless delete_pointers_by_exponent[e]
        delete_pointers_by_exponent[e].not_nil! << pointer
      end

      r = Array(Id).new(add.size) { {0_u8, 0_u64} }
      (0_u8..31).each do |e|
        e_add = add_data_by_exponent[e]
        e_delete = delete_pointers_by_exponent[e]
        if e_add || e_delete
          data_aligned_list(e)
            .update(add: (e_add ? e_add.map &.[:data] : [] of Bytes), delete: e_delete)
            .each_with_index { |pointer, add_index| r[e_add.not_nil![add_index][:data_index]] = {e, pointer} }
        end
      end
      ::Log.debug { "RoundDataStorage.update => #{r}" }
      r
    end

    def get(id : Id)
      ::Log.debug { "RoundDataStorage.get #{id}" }

      al = @data_aligned_lists_by_size_exponent[id[0]]
      return unless al

      size_and_data_encoded = IO::Memory.new al.get id[1]
      Lawn.decode_bytes_with_size size_and_data_encoded, @data_size_size
    end

    def round_exponent(size : Int32) : Int32
      31 - size.leading_zeros_count + (size.popcount == 1 ? 0 : 1)
    end
  end
end
