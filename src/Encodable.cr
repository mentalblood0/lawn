module Lawn
  module Codable
    macro included
      def initialize(bytes : Bytes)
        {% verbatim do %}
          {% begin %}
            {% i = 0 %}
            {% for v in @type.instance_vars %}
              {% if v.type == UInt32 %}
                @{{v.name.id}} = IO::ByteFormat::LittleEndian.decode UInt32, bytes[{{i}}..{{i + 4 - 1}}]
                {% i += 4 %}
              {% end %}
            {% end %}
          {% end %}
        {% end %}
      end

      def encode(io : IO)
        {% verbatim do %}
          {% begin %}
            {% for v in @type.instance_vars %}
              {% if v.type == UInt32 %}
                io.write_bytes {{v}}, IO::ByteFormat::LittleEndian
              {% end %}
            {% end %}
          {% end %}
        {% end %}
      end
    end
  end
end
