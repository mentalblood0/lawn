module Lawn
  module Codable
    macro included
      def initialize(io : IO)
        {% verbatim do %}
          {% begin %}
            {% i = 0 %}
            {% for v in @type.instance_vars %}
              {% s = {
                   Int8   => 1,
                   Int16  => 2,
                   Int32  => 4,
                   Int64  => 8,
                   UInt8  => 1,
                   UInt16 => 2,
                   UInt32 => 4,
                   UInt64 => 8,
                 }[v.type] %}
              {% if s %}
                @{{v}} = io.read_bytes {{v.type}}, IO::ByteFormat::BigEndian
              {% elsif v.type.name.starts_with? "StaticArray(UInt8," %}
                {% s = v.type.type_vars[1] %}
                b = Bytes.new {{s}}
                io.read_fully b
                @{{v}} = {{v.type}}.new 0
                b.to_unsafe.copy_to pointerof(@{{v}}).as(UInt8*), {{s}}
              {% end %}
              {% i += s %}
            {% end %}
          {% end %}
        {% end %}
      end

      def encode(io : IO)
        {% verbatim do %}
          {% begin %}
            {% for v in @type.instance_vars %}
              {% if [Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64].includes? v.type %}
                io.write_bytes {{v}}, IO::ByteFormat::BigEndian
              {% elsif v.type.name(generic_args: false) == "StaticArray" %}
                io.write {{v}}.to_slice
              {% end %}
            {% end %}
          {% end %}
        {% end %}
      end
    end
  end
end
