require "log"
require "spec"

require "../src/Env"
require "../src/SplitDataStorage"

struct Slice(T)
  def pretty_print(pp : PrettyPrint)
    pp.text "Bytes[#{self.hexstring}]"
  end
end

alias Config = {env: Lawn::Env, seed: Int32}

config = Config.from_yaml File.read ENV["SPEC_CONFIG_PATH"]
rnd = Random.new config[:seed]

describe Lawn do
  it "encodes/decodes numbers encoded in arbitrary number of bytes" do
    io = IO::Memory.new
    (1_u8..8).each do |size_size|
      (0_u64..Math.min 2_u64 ** 12, 2_u64 ** ((size_size == 8) ? 63 : (size_size * 8)) - 2).each do |size|
        Lawn.encode_number io, size, size_size
        io.rewind
        (Lawn.decode_number io, size_size).should eq size
        io.rewind
      end
    end
  end
  it "encodes/decodes bytes with sizes" do
    io = IO::Memory.new
    Lawn.encode_bytes_with_size io, "lalala".to_slice, 1_u8
    io.rewind
    (Lawn.decode_bytes_with_size io, 1_u8).should eq "lalala".to_slice
    io.rewind
    Lawn.encode_bytes_with_size io, "lalala".to_slice, 3_u8
    io.rewind
    (Lawn.decode_bytes_with_size io, 3_u8).should eq "lalala".to_slice
    io.rewind
    Lawn.encode_bytes_with_size io, nil, 3_u8
    io.rewind
    (Lawn.decode_bytes_with_size io, 3_u8).should eq nil
  end
end

describe Lawn::AlignedList do
  it "raises on invalid element size" do
    al = Lawn::AlignedList.new IO::Memory.new, 5
    expect_raises(Lawn::AlignedList::Exception) { al.add [Bytes.new 6] }
  end

  it "simple test", focus: true do
    al = Lawn::AlignedList.new IO::Memory.new, (1 << 9).to_u32
    data = [Bytes.new(512) { 1_u8 }, Bytes.new(505) { 2_u8 }, Bytes.new(512) { 3_u8 }]
    al.add(data).each_with_index { |pointer, data_index| al.get(pointer).should eq data[data_index] }
  end

  [2, 3, 5, 9].map { |s| s.to_u8! }.each do |s|
    it "generative test: supports #{s} bytes elements" do
      al = Lawn::AlignedList.new IO::Memory.new, s
      l = Hash(UInt64, Bytes).new

      1000.times do
        case rnd.rand 0..2
        when 0
          b = rnd.random_bytes s
          (al.add [b]).each { |p| l[p] = b }
        when 1
          k = l.keys.sample rnd rescue next
          al.delete k
          l.delete k
        when 2
          k = l.keys.sample rnd rescue next
          b = rnd.random_bytes s
          al.replace k, b
          l[k] = b
        end
        Log.debug { "{" + (l.map { |i, b| "#{i}: #{b.hexstring}" }.join ' ') + "}" }
        l.each { |i, b| (al.get i).should eq b }
      end
    end
  end
end

describe Lawn::SplitDataStorage do
  sds = config[:env].split_data_storage

  it "splits correctly" do
    (1_i32..2**20).each do |n|
      ((sds.split n).sum >= n).should eq true
    end
    (UInt32::MAX // 2 - 1024 * 10..UInt32::MAX // 2).each do |n|
      ((sds.split n).sum >= n).should eq true
    end
  end

  it "simple test" do
    data = Array(Bytes).new(5) { rnd.random_bytes rnd.rand 1..1024 }
    (sds.add data).each_with_index { |pointer, data_index| sds.get(pointer).should eq data[data_index] }
  end

  it "generative test" do
    added = Hash(UInt64, Bytes).new
    100.times do
      case rnd.rand 0..1
      when 0
        data = Array(Bytes).new(rnd.rand 1..16) { rnd.random_bytes rnd.rand 1..1024 }
        (sds.add data).each_with_index { |pointer, data_index| added[pointer] = data[data_index] }
      when 1
        pointer = added.keys.sample rnd rescue next
        sds.delete pointer
        added.delete pointer
      end
    end
    added.each do |pointer, data|
      (sds.get pointer).should eq data
    end
  end
end

describe Lawn::Env do
  env = config[:env]

  it "generative test" do
    h = Hash(Lawn::K, Lawn::V).new

    ks = 0..1024
    vs = 0..1024
    100.times do
      # Log.debug { "data: #{(File.read env.sst.data.path).to_slice.hexstring}" }
      case rnd.rand 0..1
      when 0
        k = rnd.random_bytes rnd.rand ks
        v = rnd.random_bytes rnd.rand vs
        Log.debug { "add #{k.hexstring} #{v.hexstring}" }

        env.transaction.set(k, v).commit

        h[k] = v
      when 1
        k = h.keys.sample rnd rescue next
        Log.debug { "delete #{k.hexstring}" }

        env.transaction.delete(k).commit

        h.delete k
      end
      h.keys.sort.each { |k| env.get(k).should eq h[k] }
    end
  end
end
