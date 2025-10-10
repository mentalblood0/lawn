require "./src/Env"

alias Config = {env: Lawn::Env, seed: Int32, amount: UInt64, key_size: UInt64, value_size: UInt64}

config = Config.from_yaml File.read ENV["BENCHMARK_CONFIG_PATH"]
env = config[:env]
rnd = Random.new config[:seed]

kv = Hash(Bytes, Bytes).new
config[:amount].times { kv[rnd.random_bytes 16] = rnd.random_bytes 32 }
time_to_write = Time.measure do
  kv.each { |k, v| env.transaction.set(k, v).commit }
end

ks = kv.keys
ks.shuffle! rnd
time_to_get = Time.measure do
  ks.each { |k| env.get k }
end

bites_written = config[:amount] * (2 + config[:key_size] + 2 + config[:value_size])

{"write" => time_to_write,
 "get"   => time_to_get,
}.each do |o, tt|
  puts "#{o}:"
  puts "\t#{(bites_written / tt.total_seconds).to_u64.humanize_bytes}/s"
  puts "\t#{(config[:amount] / tt.total_seconds).to_u64.humanize}r/s"
  puts "\t#{tt.total_seconds.humanize}s passed"
end

puts "#{bites_written}B (#{bites_written.humanize_bytes}) written"
