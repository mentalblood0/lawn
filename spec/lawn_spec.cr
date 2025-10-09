require "log"
require "spec"

require "../src/Env"
require "../src/SplitDataStorage"
require "../src/Codable"

alias Config = {env: Lawn::Env, seed: Int32}

config = Config.from_yaml File.read ENV["SPEC_CONFIG_PATH"]
rnd = Random.new config[:seed]

record Example,
  i8 : Int8,
  i16 : Int16,
  i32 : Int32,
  i64 : Int64,
  u8 : UInt8,
  u16 : UInt16,
  u32 : UInt32,
  u64 : UInt64,
  a : StaticArray(UInt8, 3),
  n : Bytes?,
  b : Bytes { include Lawn::Codable }

describe Lawn::Codable do
  it "encodes/decodes" do
    e = Example.new(
      i8: Int8::MIN,
      i16: Int16::MIN,
      i32: Int32::MIN,
      i64: Int64::MIN,
      u8: UInt8::MAX,
      u16: UInt16::MAX,
      u32: UInt32::MAX,
      u64: UInt64::MAX,
      a: UInt8.static_array(1_u8, 2_u8, 3_u8),
      n: nil,
      b: "lalala".to_slice)

    io = IO::Memory.new
    e.encode io

    io.rewind
    (Example.new io).should eq e
  end
end

describe Lawn::SplitDataStorage do
  sds = config[:env].split_data_storage

  it "splits correctly" do
    (1_i32..2**20).each do |n|
      ((sds.fast_split n).sum >= n).should eq true
    end
    (UInt32::MAX // 2 - 1024 * 10..UInt32::MAX // 2).each do |n|
      ((sds.fast_split n).sum >= n).should eq true
    end
  end
end

describe Lawn::AlignedList do
  it "raises on invalid element size" do
    al = Lawn::AlignedList.new IO::Memory.new, 5
    expect_raises(Lawn::AlignedList::Exception) { al.add Bytes.new 4 }
    expect_raises(Lawn::AlignedList::Exception) { al.add Bytes.new 6 }
  end

  [2, 3, 5, 9].map { |s| s.to_u8! }.each do |s|
    it "supports #{s} bytes elements" do
      al = Lawn::AlignedList.new IO::Memory.new, s
      l = Hash(UInt64, Bytes).new

      1000.times do
        case rnd.rand 0..2
        when 0
          b = rnd.random_bytes s
          l[al.add b] = b
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

describe Lawn::Env do
  env = config[:env]

  it "generative test" do
    h = Hash(Bytes, Bytes?).new

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
