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
  a : StaticArray(UInt8, 3) { include Lawn::Codable }

describe Lawn::Codable do
  it "encodes/decodes" do
    e = Example.new(
      i8: 1_i8,
      i16: 2_i16,
      i32: 3_i32,
      i64: 4_i64,
      u8: 5_u8,
      u16: 6_u16,
      u32: 7_u32,
      u64: 8_u64,
      a: UInt8.static_array 1_u8, 2_u8, 3_u8)

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
