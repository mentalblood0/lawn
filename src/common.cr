require "log"

module Lawn
  macro mserializable
    include YAML::Serializable
    include YAML::Serializable::Strict
    include JSON::Serializable
    include JSON::Serializable::Strict
  end

  alias K = Bytes
  alias V = Bytes

  module IOConverter
    alias Args = NamedTuple(
      file: NamedTuple(
        filename: Path | String,
        mode: String,
        perm: File::Permissions),
      sync: Bool)

    def self.from_yaml(ctx : YAML::ParseContext, node : YAML::Nodes::Node) : IO::Memory | File
      begin
        args = Args.new ctx, node
        Dir.mkdir_p (Path.new args[:file][:filename]).parent
        r = File.new **args[:file]
        r.sync = args[:sync]
        r
      rescue YAML::ParseException
        node.raise "Expected #{Args} or String of value \"memory\", not #{node.kind}" unless ((String.new ctx, node) == "memory" rescue false)
        IO::Memory.new
      end
    end

    def self.to_yaml(value : IO | File, builder : YAML::Nodes::Builder)
      case value
      when IO::Memory then "memory".to_yaml builder
      when File       then {file: {filename: value.path, perm: value.info.permissions}, sync: value.sync?}.to_yaml builder
      else
        raise "Unsupported IO type: #{value.class}"
      end
    end
  end
end
