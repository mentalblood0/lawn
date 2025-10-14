require "yaml"
require "json"

require "./common"
require "./AlignedList"

module Lawn
  class RoundDataStorage
    Lawn.mserializable

    getter pointer_size : UInt8
    getter data_dir : String

    @[YAML::Field(ignore: true)]
    getter data_aligned_lists_by_size_exponent : Array(AlignedList?) = Array(AlignedList?).new 32 { nil }

    def data_aligned_list(size_exponent : UInt8)
      unless data_aligned_lists_by_size_exponent[size_exponent]
        io = File.new (Path.new @data_dir) / "data_of_size_with_rounded_up_exponent_#{size_exponent.to_s.rjust 2, '0'}.dat", "w+"
        io.sync = true
        data_aligned_lists_by_size_exponent[size_exponent] = AlignedList.new io, (1_u32 << size_exponent)
      end
      data_aligned_lists_by_size_exponent[size_exponent].not_nil!
    end

    def initialize(@pointer_size, @data_dir)
    end

    alias Id = {UInt8, UInt64}

    def update(add : Array(Bytes), delete : Array(Id)) : Array(Id)
      ::Log.debug { "RoundDataStorage.update add: #{add.map &.hexstring}, delete: #{delete}" }

      add_data_by_exponent = Array(Array(Bytes)?).new(32) { nil }
      add.each do |data|
        e = round_exponent data.size
        add_data_by_exponent[e] = Array(Bytes).new unless add_data_by_exponent[e]
        add_data_by_exponent[e].not_nil! << data
      end

      delete_pointers_by_exponent = Array(Array(UInt64)?).new(32) { nil }
      delete.each do |e, pointer|
        delete_pointers_by_exponent[e] = Array(UInt64).new unless delete_pointers_by_exponent[e]
        delete_pointers_by_exponent[e].not_nil! << pointer
      end

      r = Array(Id).new
      (0_u8..31).each do |e|
        e_add = add_data_by_exponent[e]
        e_delete = delete_pointers_by_exponent[e]
        if e_add || e_delete
          r.concat data_aligned_list(e).update(add: e_add.not_nil!, delete: e_delete).map { |pointer| {e, pointer} }
        end
      end
      r
    end

    def get(id : Id)
      ::Log.debug { "RoundDataStorage.get #{id}" }

      al = @data_aligned_lists_by_size_exponent[id[0]]
      al.get id[1] if al
    end

    def round_exponent(size : Int32) : Int32
      31 - size.leading_zeros_count + (size.popcount == 1 ? 0 : 1)
    end
  end
end
