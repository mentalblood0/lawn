module Lawn
  module Codable
    annotation Field
    end

    macro included
      def initialize(io : IO)
        {% verbatim do %}
          {% begin %}
            {% for v in @type.instance_vars %}
              {% if [Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64].includes? v.type %}
                @{{v}} = io.read_bytes {{v.type}}, IO::ByteFormat::BigEndian
              {% elsif v.type.name.starts_with? "StaticArray(UInt8," %}
                {% s = v.type.type_vars[1] %}
                b = Bytes.new {{s}}
                io.read_fully b
                @{{v}} = {{v.type}}.new 0
                b.copy_to pointerof(@{{v}}).as(UInt8*), {{s}}
              {% elsif v.type.name.starts_with? "Slice(UInt8)" %}
                s = IO::ByteFormat::BigEndian.decode UInt16, io
                {% if v.type == Bytes? %}
                  @{{v}} = nil if s == UInt16::MAX
                {% end %}
                @{{v}} = Bytes.new s
                io.read_fully @{{v}}
              {% end %}
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
              {% elsif v.type.name.starts_with? "StaticArray(UInt8," %}
                io.write {{v}}.to_slice
              {% elsif v.type.name.starts_with? "Slice(UInt8)" %}
                io.write_bytes UInt16::MAX, IO::ByteFormat::BigEndian unless @{{v}}
                io.write_bytes @{{v}}.size.to_u16, IO::ByteFormat::BigEndian
                io.write @{{v}}
              {% end %}
            {% end %}
          {% end %}
        {% end %}
      end
    end
  end
end
