require "log"
require "spec"

require "../src/Database"
require "../src/RoundDataStorage"
require "../src/AVLTree"

struct Slice(T)
  def pretty_print(pp : PrettyPrint)
    pp.text "Bytes[#{self.hexstring}]"
  end
end

alias Config = {database: Lawn::Database, seed: Int32}

config = Config.from_yaml File.read ENV["SPEC_CONFIG_PATH"]
rnd = Random.new config[:seed]

Spec.before_each { config[:database].clear }

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
  it "correctly deletes all the elements" do
    amount = 16
    element_size = 16
    aligned_list = Lawn::AlignedList.new config[:database].log.path.parent / "aligned_list.dat", element_size
    data = Array(Bytes).new(amount) { rnd.random_bytes element_size }

    ids = aligned_list.update data
    ids.each_with_index { |id, data_index| aligned_list.get(id).should eq data[data_index] }

    aligned_list.update [] of Bytes, ids

    ids = aligned_list.update data
    ids.each_with_index { |id, data_index| aligned_list.get(id).should eq data[data_index] }
  end

  [2, 3, 5, 9].map { |s| s.to_u8! }.each do |s|
    it "generative test: supports #{s} bytes elements" do
      al = Lawn::AlignedList.new config[:database].log.path.parent / "aligned_list.dat", s
      added = Hash(Int64, Bytes).new

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

data_storage = config[:database].tables.first.data_storage
case data_storage
when Lawn::RoundDataStorage
  describe Lawn::RoundDataStorage do
    it "correctly splits sizes scale" do
      points = 256
      (256..2**(8 * 2)).step(19).each do |max|
        r = Lawn::RoundDataStorage.get_sizes max, points
        r.sort.should eq r
        r.last.should eq max
        r.size.should eq points
      end
    end

    it "simple test" do
      add = Array(Bytes).new(2) { rnd.random_bytes rnd.rand 1..16 }
      (data_storage.update add, [] of Lawn::RoundDataStorage::Id).each_with_index { |id, data_index| data_storage.get(id).should eq add[data_index] }
    end

    it "generative test" do
      added = Hash(Lawn::RoundDataStorage::Id, Bytes).new
      1000.times do
        add = Array(Bytes).new(rnd.rand 1..16) { rnd.random_bytes rnd.rand 1..1024 }
        delete = added.keys.sample rnd.rand(1..16), rnd
        r = data_storage.update add, delete
        r.each_with_index { |pointer, data_index| added[pointer] = add[data_index] }
        delete.each { |pointer| added.delete pointer }
      end
      added.each do |pointer, data|
        (data_storage.get pointer).should eq data
      end
    end
  end
end

describe Lawn::AVLTree do
  it "generative test" do
    tree = Lawn::AVLTree.new
    added = Hash(Bytes, Bytes?).new
    1000.times do
      case rnd.rand 0..4
      when 0, 1, 2
        key = rnd.random_bytes rnd.rand 1..1024
        value = rnd.random_bytes rnd.rand 1..1024
        ::Log.debug { "add #{key.hexstring} : #{value.hexstring}" }
        tree[key] = value
        added[key] = value
      when 3
        key = added.keys.sample rnd rescue next
        ::Log.debug { "delete #{key.hexstring}" }
        tree.delete key
        added.delete key
      when 4
        key = added.keys.sample rnd rescue next
        ::Log.debug { "set value = nil #{key.hexstring}" }
        tree[key] = nil
        added[key] = nil
      end
      added.each { |key, value| tree[key]?.should eq value }
    end
    sorted = added.to_a.sort_by { |key, _| key }
    tree.each.should eq sorted
    tree.each { |key, value| tree.each(from: key).should eq sorted[sorted.index({key, value})..] }
  end
end

describe Lawn::Database do
  database = config[:database]

  it "checkpoints" do
    key = "key".to_slice
    value = "value".to_slice
    database.transaction.set(0_u8, key, value).commit
    database.checkpoint
    database.tables.first.get(key).should eq value
  end

  it "handles deletes correctly" do
    database.transaction.set(0_u8, [{"key_to_delete".to_slice, "value".to_slice},
                                    {"key".to_slice, "value".to_slice}]).commit
    database.checkpoint
    database.tables.first.get("key_to_delete".to_slice).should eq "value".to_slice
    database.tables.first.get("key".to_slice).should eq "value".to_slice

    database.transaction.delete(0_u8, "key_to_delete".to_slice).commit
    database.checkpoint
    database.tables.first.get("key_to_delete".to_slice).should eq nil
    database.tables.first.get("key".to_slice).should eq "value".to_slice
  end

  it "handles updates correctly" do
    key = "key".to_slice
    value = "value".to_slice
    new_value = "new_value".to_slice

    database.transaction.set(0_u8, key, value).commit
    database.checkpoint
    database.tables.first.get(key).should eq value

    database.transaction.set(0_u8, key, new_value).commit
    database.checkpoint
    database.tables.first.get(key).should eq new_value
  end

  it "generative test" do
    added = Array(Hash(Lawn::Key, Lawn::Value)).new(database.tables.size) { Hash(Lawn::Key, Lawn::Value).new }
    200.times do
      rnd.rand(1..16).times do
        table_id = rnd.rand(0..(database.tables.size - 1)).to_u8

        case table = database.tables[table_id]
        when Lawn::VariableTable
          key_size = rnd.rand 1..16
          value_size = rnd.rand 1..16
        when Lawn::FixedTable
          key_size = table.key_size
          value_size = table.value_size
        else
          raise "unreacheable"
        end

        case rnd.rand 0..2
        when 0, 1
          key = rnd.random_bytes key_size
          value = rnd.random_bytes value_size

          database.transaction.set(table_id, key, value).commit
          added[table_id][key] = value
        when 2
          key = added[table_id].keys.sample rnd rescue next

          database.transaction.delete(table_id, key).commit
          added[table_id].delete key
        end
      end
      database.checkpoint
      added.each_with_index do |added_in_table, table_id|
        added_in_table.keys.each { |k| database.tables[table_id].get(k).should eq added_in_table[k] }
      end
    end
    added.each_with_index do |added_in_table, table_id|
      all_added_in_table = added_in_table.to_a.sort_by { |key, _| key }
      all_present_in_table = database.tables[table_id].each
      all_present_in_table.should eq all_added_in_table
      all_present_in_table.each do |key, value|
        database.tables[table_id].each(from: key).should eq all_added_in_table[all_added_in_table.index({key, value})..]
      end
    end
  end
end
