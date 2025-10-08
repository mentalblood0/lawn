require "log"
require "spec"

require "../src/Env"
require "../src/SplitDataStorage"

alias Config = {env: Lawn::Env, seed: Int32}

config = Config.from_yaml File.read ENV["SPEC_CONFIG_PATH"]
rnd = Random.new config[:seed]

describe Lawn::SplitDataStorage do
  sds = Lawn::SplitDataStorage.new 4_u8, 5_u8

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
