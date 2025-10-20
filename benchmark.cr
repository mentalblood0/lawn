require "./src/Database"
require "./src/common"
require "./src/RoundDataStorage"

macro write_speeds
  puts "\tdata speed: #{(total_size / time.total_seconds).to_u64.humanize_bytes}/s"
  puts "\trecords speed: #{(config[:records] / time.total_seconds).to_u64.humanize}r/s"
end

macro random_fixed_key
  rnd.random_bytes rnd.rand config[:size][:key][:fixed]..config[:size][:key][:fixed]
end

macro random_fixed_value
  rnd.random_bytes rnd.rand config[:size][:value][:fixed]..config[:size][:value][:fixed]
end

macro random_key
  rnd.random_bytes rnd.rand config[:size][:key][:min]..config[:size][:key][:max]
end

macro random_value
  rnd.random_bytes rnd.rand config[:size][:value][:min]..config[:size][:value][:max]
end

macro random_data
  rnd.random_bytes rnd.rand (config[:size][:key][:min] + config[:size][:value][:min])..(config[:size][:key][:max] + config[:size][:value][:max])
end

alias Config = {benchmarks: Array(String), database: Lawn::Database, seed: Int32, records: Int64, records_per_transaction: Int64, size: {key: {min: UInt64, max: UInt64}, value: {min: UInt64, max: UInt64}}}

config = Config.from_yaml File.read ENV["BENCHMARK_CONFIG_PATH"]
rnd = Random.new config[:seed]

if config[:benchmarks].any? { |benchmark_name| benchmark_name.starts_with? "database table " }
  rnd = Random.new config[:seed]

  keyvalues = {} of Bytes => Bytes

  table_id = 0_u8
  table = config[:database].tables[table_id].as Lawn::Table
  config[:records].times { keyvalues[random_key] = random_value }
  total_size = keyvalues.map { |key, value| key.size.to_i64 + value.size }.sum

  if config[:benchmarks].includes? "database table add"
    puts "database table write #{keyvalues.size} key-value pairs of total size #{total_size.humanize_bytes}"
    time = Time.measure { keyvalues.each_slice(config[:records_per_transaction]) { |slice| config[:database].transaction.set(table_id, slice).commit } }
    write_speeds
  end

  if config[:benchmarks].includes? "database table checkpoint"
    puts "database table checkpoint #{keyvalues.size} key-value pairs of total size #{total_size.humanize_bytes}"
    time = Time.measure { config[:database].checkpoint }
    write_speeds
  end

  if config[:benchmarks].includes? "database table random get"
    puts "database table random get #{keyvalues.size} key-value pairs of total size #{total_size.humanize_bytes}"
    rnd = Random.new config[:seed]
    keys = keyvalues.keys
    keys.shuffle! rnd
    time = Time.measure { keys.each { |key| config[:database].tables[table_id].get key } }
    write_speeds
  end
end

if config[:benchmarks].any? { |benchmark_name| benchmark_name.starts_with? "database fixed table " }
  rnd = Random.new config[:seed]

  keyvalues = {} of Bytes => Bytes

  table_id = 1_u8
  table = config[:database].tables[table_id].as Lawn::FixedTable
  config[:records].times { keyvalues[rnd.random_bytes table.key_size] = rnd.random_bytes table.value_size }
  total_size = keyvalues.size * (table.key_size + table.value_size)

  if config[:benchmarks].includes? "database fixed table add"
    puts "database fixed table write #{keyvalues.size} key-value pairs of total size #{total_size.humanize_bytes}"
    time = Time.measure { keyvalues.each_slice(config[:records_per_transaction]) { |slice| config[:database].transaction.set(table_id, slice).commit } }
    write_speeds
  end

  if config[:benchmarks].includes? "database fixed table checkpoint"
    puts "database fixed table checkpoint #{keyvalues.size} key-value pairs of total size #{total_size.humanize_bytes}"
    time = Time.measure { config[:database].checkpoint }
    write_speeds
  end

  if config[:benchmarks].includes? "database fixed table random get"
    puts "database fixed table random get #{keyvalues.size} key-value pairs of total size #{total_size.humanize_bytes}"
    rnd = Random.new config[:seed]
    keys = keyvalues.keys
    keys.shuffle! rnd
    time = Time.measure { keys.each { |key| config[:database].tables[table_id].get key } }
    write_speeds
  end
end

if config[:benchmarks].any? { |benchmark_name| benchmark_name.starts_with? "aligned list" }
  rnd = Random.new config[:seed]
  element_size = 256
  aligned_list = Lawn::AlignedList.new config[:database].tables.first.as(Lawn::Table).data_storage.dir / "benchmark_aligned_list.dat", 256
  amount = config[:records]
  add = Array.new(amount) { rnd.random_bytes element_size }
  ids = [] of Int64
  total_size = (add.map &.size.to_u64).sum

  if config[:benchmarks].includes? "aligned list add/delete"
    puts "aligned list add #{amount} data of total size #{total_size.humanize_bytes}"
    time = Time.measure { ids = aligned_list.update add }
    write_speeds

    puts "aligned list delete #{amount} data of total size #{total_size.humanize_bytes}"
    rnd = Random.new config[:seed]
    shuffled_ids = ids.shuffle rnd
    time = Time.measure { ids = aligned_list.update [] of Bytes, shuffled_ids }
    write_speeds

    puts "aligned list add after delete #{amount} data of total size #{total_size.humanize_bytes}"
    time = Time.measure { ids = aligned_list.update add }
    write_speeds
  end

  if config[:benchmarks].includes? "aligned list random get"
    puts "aligned list random get #{amount} data of total size #{total_size.humanize_bytes}"
    aligned_list.update add
    rnd = Random.new config[:seed]
    shuffled_ids = ids.shuffle rnd
    time = Time.measure { shuffled_ids.each { |id| aligned_list.get id } }
    write_speeds
  end
end

if config[:benchmarks].any? { |benchmark_name| benchmark_name.starts_with? "data storage" }
  rnd = Random.new config[:seed]
  data_storage = Lawn::RoundDataStorage.new config[:database].tables.first.as(Lawn::Table).data_storage.dir / "benchmark_data_storage", max_element_size: 65536
  amount = config[:records]
  add = Array.new(amount) { random_data }
  ids = [] of Lawn::RoundDataStorage::Id
  total_size = (add.map &.size.to_u64).sum

  if config[:benchmarks].includes? "data storage add"
    puts "data storage add #{amount} data of total size #{total_size.humanize_bytes}"
    time = Time.measure { ids = data_storage.update add, [] of Lawn::RoundDataStorage::Id }
    write_speeds
  end

  if config[:benchmarks].includes? "data storage random get"
    puts "data storage random get #{amount} data of total size #{total_size.humanize_bytes}"
    rnd = Random.new config[:seed]
    ids.shuffle! rnd
    time = Time.measure { ids.each { |id| data_storage.get id } }
    write_speeds
  end
end
