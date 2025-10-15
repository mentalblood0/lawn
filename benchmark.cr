require "./src/Env"
require "./src/common"
require "./src/RoundDataStorage"

alias Result = {data_speed: String, records_speed: String, time: String}

class Benchmarks
  Lawn.mserializable

  getter env : Lawn::Env
  getter seed : Int32
  getter amount : UInt64
  getter size : {key: {min: UInt64, max: UInt64}, value: {min: UInt64, max: UInt64}}

  @[YAML::Field(ignore: true)]
  getter results : Hash(String, Result) = {} of String => Result

  @[YAML::Field(ignore: true)]
  getter kv : Hash(Bytes, Bytes) = {} of Bytes => Bytes

  @[YAML::Field(ignore: true)]
  getter rnd : Random { Random.new @seed }

  getter keyvalues

  def add(name : String, time : Time::Span, bytes_written : UInt64, amount : UInt64? = nil)
    amount = @amount unless amount
    @results[name] = {data_speed:    "#{(bytes_written / time.total_seconds).to_u64.humanize_bytes}/s",
                      records_speed: "#{(amount / time.total_seconds).to_u64.humanize}r/s",
                      time:          "#{time.total_seconds.humanize}s passed"}
  end

  def random_key
    rnd.random_bytes rnd.rand @size[:key][:min]..@size[:key][:max]
  end

  def random_value
    rnd.random_bytes rnd.rand @size[:value][:min]..@size[:value][:max]
  end

  def benchmark_write
    @kv.clear
    @amount.times { kv[random_key] = random_value }
    add "Env.write #{kv.size} key-value pairs of total size #{kv.map { |k, v| k.size + v.size }.sum.humanize_bytes}", Time.measure { kv.each { |k, v| @env.transaction.set(k, v).commit } }, @kv.map { |key, value| key.size.to_u64 + value.size }.sum
  end

  def benchmark_checkpointing
    add "Env.checkpoint #{kv.size} key-value pairs of total size #{kv.map { |k, v| k.size + v.size }.sum.humanize_bytes}", Time.measure { @env.checkpoint }, @kv.map { |key, value| key.size.to_u64 + value.size }.sum
  end

  def benchmark_get
    ks = @kv.keys
    ks.shuffle! rnd
    add "Env.get #{kv.size} key-value pairs of total size #{kv.map { |k, v| k.size + v.size }.sum.humanize_bytes}", Time.measure { ks.each { |k| env.get k } }, @kv.map { |_, value| value.size.to_u64 }.sum
  end

  def benchmark_data_storage
    ds = @env.data_storage
    amount = @amount * 2
    add = Array.new(amount) { random_key }
    ids = [] of Lawn::RoundDataStorage::Id
    total_size = (add.map &.size.to_u64).sum
    add "#{ds.class}: add #{amount} data of total size #{total_size.humanize_bytes}", Time.measure { ids = ds.update add, [] of Lawn::RoundDataStorage::Id }, total_size, amount

    add "#{ds.class}: get #{amount} data of total size #{total_size.humanize_bytes}", Time.measure { ids.each { |id| ds.get id } }, total_size, amount
  end
end

benchmarks = Benchmarks.from_yaml File.read ENV["BENCHMARK_CONFIG_PATH"]
benchmarks.benchmark_write
benchmarks.benchmark_checkpointing
benchmarks.benchmark_get
# benchmarks.benchmark_data_storage

puts benchmarks.results.to_yaml
