require "./src/Env"
require "./src/common"

alias Result = {data_speed: String, records_speed: String, time: String}

class Benchmarks
  Lawn.mserializable

  getter env : Lawn::Env
  getter seed : Int32
  getter amount : UInt64
  getter key_size : UInt64
  getter value_size : UInt64

  @[YAML::Field(ignore: true)]
  getter results : Hash(String, Result) = {} of String => Result

  @[YAML::Field(ignore: true)]
  getter kv : Hash(Bytes, Bytes) = {} of Bytes => Bytes

  def add(name : String, time : Time::Span, bytes_written : UInt64? = nil, amount : UInt64? = nil)
    bytes_written = @amount * (2 + @key_size + 2 + @value_size) unless bytes_written
    amount = @amount unless amount
    @results[name] = {data_speed:    "#{(bytes_written / time.total_seconds).to_u64.humanize_bytes}/s",
                      records_speed: "#{(amount / time.total_seconds).to_u64.humanize}r/s",
                      time:          "#{time.total_seconds.humanize}s passed"}
  end

  def benchmark_write
    rnd = Random.new @seed
    @kv.clear
    @amount.times { kv[rnd.random_bytes 16] = rnd.random_bytes 32 }
    add "Env.write #{kv.size} key-value pairs of total size #{kv.map { |k, v| k.size + v.size }.sum.humanize_bytes}", Time.measure { kv.each { |k, v| @env.transaction.set(k, v).commit } }
  end

  def benchmark_checkpointing
    add "Env.checkpoint #{kv.size} key-value pairs of total size #{kv.map { |k, v| k.size + v.size }.sum.humanize_bytes}", Time.measure { @env.checkpoint }
  end

  def benchmark_get
    rnd = Random.new @seed
    ks = @kv.keys
    ks.shuffle! rnd
    add "Env.get #{kv.size} key-value pairs of total size #{kv.map { |k, v| k.size + v.size }.sum.humanize_bytes}", Time.measure { ks.each { |k| env.get k } }
  end

  def benchmark_split_data_storage
    rnd = Random.new @seed
    sds = @env.split_data_storage
    amount = @amount * 2
    add = Array.new(amount) { rnd.random_bytes rnd.rand 1..1024 }
    total_size = (add.map &.size.to_u64).sum
    add "SplitDataStorage: add #{amount} data of total size #{total_size.humanize_bytes}", Time.measure { sds.update add, [] of UInt64 }, total_size, amount

    add "SplitDataStorage: get #{amount} data of total size #{total_size.humanize_bytes}", Time.measure { (1_u64..add.size).each { |p| sds.get p } }, total_size, amount
  end
end

benchmarks = Benchmarks.from_yaml File.read ENV["BENCHMARK_CONFIG_PATH"]
benchmarks.benchmark_write
benchmarks.benchmark_checkpointing
benchmarks.benchmark_get
# benchmarks.benchmark_split_data_storage

puts benchmarks.results.to_yaml
