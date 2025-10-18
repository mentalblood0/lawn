require "./src/Env"
require "./src/common"
require "./src/RoundDataStorage"

macro write_speeds
  puts "\tdata speed: #{(total_size / time.total_seconds).to_u64.humanize_bytes}/s"
  puts "\trecords speed: #{(config[:amount] / time.total_seconds).to_u64.humanize}r/s"
end

macro random_key
  rnd.random_bytes rnd.rand config[:size][:key][:min]..config[:size][:key][:max]
end

macro random_value
  rnd.random_bytes rnd.rand config[:size][:value][:min]..config[:size][:value][:max]
end

alias Config = {benchmarks: Array(String), env: Lawn::Env, seed: Int32, amount: Int64, size: {key: {min: UInt64, max: UInt64}, value: {min: UInt64, max: UInt64}}}

config = Config.from_yaml File.read ENV["BENCHMARK_CONFIG_PATH"]
rnd = Random.new config[:seed]

if config[:benchmarks].any? { |benchmark_name| benchmark_name.starts_with? "env " }
  keyvalues = {} of Bytes => Bytes
  config[:amount].times { keyvalues[random_key] = random_value }
  total_size = keyvalues.map { |key, value| key.size.to_i64 + value.size }.sum

  if config[:benchmarks].includes? "env add"
    puts "env write #{keyvalues.size} key-value pairs of total size #{total_size.humanize_bytes}"
    time = Time.measure { keyvalues.each { |key, value| config[:env].transaction.set(key, value).commit } }
    write_speeds
  end

  if config[:benchmarks].includes? "env checkpoint"
    puts "env checkpoint #{keyvalues.size} key-value pairs of total size #{total_size.humanize_bytes}"
    time = Time.measure { config[:env].checkpoint }
    write_speeds
  end

  if config[:benchmarks].includes? "env random get"
    puts "env random get #{keyvalues.size} key-value pairs of total size #{total_size.humanize_bytes}"
    rnd = Random.new config[:seed]
    keys = keyvalues.keys
    keys.shuffle! rnd
    time = Time.measure { keys.each { |key| config[:env].get key } }
    write_speeds
  end
end

if config[:benchmarks].any? { |benchmark_name| benchmark_name.starts_with? "data storage" }
  data_storage = Lawn::RoundDataStorage.new config[:env].data_storage.dir / "benchmark_data_storage", {max: 65534, points: 327}
  amount = config[:amount] * 2
  add = Array.new(amount) { random_key }
  ids = [] of Lawn::RoundDataStorage::Id
  total_size = (add.map &.size.to_u64).sum

  if config[:benchmarks].includes? "data storage add"
    puts "data storage add #{amount} data of total size #{total_size.humanize_bytes}"
    rnd = Random.new config[:seed]
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
