require "log"
require "spec"

require "../src/Env"
require "../src/SplitDataStorage"
require "../src/RoundDataStorage"

struct Slice(T)
  def pretty_print(pp : PrettyPrint)
    pp.text "Bytes[#{self.hexstring}]"
  end
end

alias Config = {env: Lawn::Env, seed: Int32}

config = Config.from_yaml File.read ENV["SPEC_CONFIG_PATH"]
rnd = Random.new config[:seed]

Spec.before_each { config[:env].clear }

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
  end
end

describe Lawn::AlignedList do
  [2, 3, 5, 9].map { |s| s.to_u8! }.each do |s|
    it "generative test: supports #{s} bytes elements" do
      al = Lawn::AlignedList.new config[:env].log.path.parent / "aligned_list.dat", s
      added = Hash(UInt64, Bytes).new

      1000.times do
        add = Array.new(rnd.rand 1..16) { rnd.random_bytes s }
        delete = added.keys.sample rnd.rand(1..16), rnd
        delete.each { |pointer| added.delete pointer } if delete
        r = al.update add, delete
        r.each_with_index { |pointer, add_index| added[pointer] = add[add_index] }
      end
      added.each do |i, b|
        (al.get i).should eq b
      end
    end
  end
end

ds = config[:env].data_storage

describe ds.class do
  case ds
  when Lawn::SplitDataStorage
    it "splits correctly" do
      (1..2**20).each do |n|
        ((ds.split n).sum >= n).should eq true
      end
      (UInt32::MAX // 2 - 1024 * 10..UInt32::MAX // 2).each do |n|
        ((ds.split n).sum >= n).should eq true
      end
    end

    it "simple test" do
      add = Array(Bytes).new(100) { rnd.random_bytes rnd.rand 1..1024 }
      (ds.update add, [] of UInt64).each_with_index { |pointer, data_index| ds.get(pointer).should eq add[data_index] }
    end

    it "generative test" do
      added = Hash(UInt64, Bytes).new
      100.times do
        add = Array(Bytes).new(rnd.rand 1..16) { rnd.random_bytes rnd.rand 1..2**5 }
        delete = added.keys.sample rnd.rand(1..16), rnd
        r = ds.update add, delete
        r.each_with_index { |pointer, data_index| added[pointer] = add[data_index] }
        delete.each { |pointer| added.delete pointer }
      end
      added.each do |pointer, data|
        (ds.get pointer).should eq data
      end
    end
  when Lawn::RoundDataStorage
    it "simple test" do
      add = Array(Bytes).new(2) { rnd.random_bytes rnd.rand 1..16 }
      (ds.update add, [] of Lawn::RoundDataStorage::Id).each_with_index { |id, data_index| ds.get(id).should eq add[data_index] }
    end

    it "generative test" do
      added = Hash(Lawn::RoundDataStorage::Id, Bytes).new
      1000.times do
        add = Array(Bytes).new(rnd.rand 1..16) { rnd.random_bytes rnd.rand 1..1024 }
        delete = added.keys.sample rnd.rand(1..16), rnd
        r = ds.update add, delete
        r.each_with_index { |pointer, data_index| added[pointer] = add[data_index] }
        delete.each { |pointer| added.delete pointer }
      end
      added.each do |pointer, data|
        (ds.get pointer).should eq data
      end
    end
  end
end

describe Lawn::Env do
  env = config[:env]

  it "checkpoints" do
    key = "lalala".to_slice
    value = "lololo".to_slice
    env.transaction.set(key, value).commit
    env.checkpoint
    env.get(key).should eq value
  end

  it "handles deletes correctly" do
    env.transaction.set([{"key_to_delete".to_slice, "value".to_slice},
                         {"key".to_slice, "value".to_slice}]).commit
    env.checkpoint
    env.transaction.delete("key_to_delete".to_slice).commit
    env.checkpoint
    env.get("key_to_delete".to_slice).should eq nil
    env.get("key".to_slice).should eq "value".to_slice
  end

  it "generative test" do
    added = Hash(Lawn::Key, Lawn::Value).new
    1000.times do
      rnd.rand(1..16).times do
        case rnd.rand 0..1
        when 0
          key = rnd.random_bytes rnd.rand 1..1024
          value = rnd.random_bytes rnd.rand 1..1024
          Log.debug { "add\n\tkey:   #{key.hexstring}\n\tvalue: #{value.hexstring}" }

          env.transaction.set(key, value).commit

          added[key] = value
        when 1
          key = added.keys.sample rnd rescue next
          Log.debug { "delete\n\tkey:   #{key.hexstring}" }

          env.transaction.delete(key).commit

          added.delete key
        end
      end
      env.checkpoint
      added.keys.sort.each { |k| env.get(k).should eq added[k] }
    end
  end
end
