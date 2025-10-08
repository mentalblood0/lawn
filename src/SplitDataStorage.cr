require "yaml"
require "json"

require "./common"

module Lawn
  class SplitDataStorage
    Lawn.mserializable

    getter data_size_size : UInt8
    getter pointer_size : UInt8

    getter header_size : UInt64 { @data_size_size + @pointer_size }

    def initialize(@data_size_size, @pointer_size)
    end

    def fast_split(n)
      fbi = n.trailing_zeros_count
      a = [(n.class.new 1) << fbi]
      asum = a[0]
      (fbi + 1..n.bit_length).each do |i|
        next unless (n.bit i) == 1
        b = (n.class.new 1) << i
        if ((a.size > 1) ? a.size * @pointer_size : 0) >= b - asum
          a = [b * 2]
          asum = b * 2
        else
          a << b
          asum += b
        end
      end
      a
    end
  end
end
