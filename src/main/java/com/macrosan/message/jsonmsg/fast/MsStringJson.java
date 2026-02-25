package com.macrosan.message.jsonmsg.fast;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class MsStringJson {
    //old
    public static void serialize1(String value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (gen instanceof MsJsonGenerator) {
            ((MsJsonGenerator) gen).verify();
            //可能有特殊字符的字段，用 !长度XXX 的方式进行序列化
            if (value.length() > 2 && value.charAt(0) == '{') {
                gen.writeRaw('!');
                int length = value.length();
                char c1 = (char) ((length >> 16) | 0x8000);
                char c2 = (char) (length & 0xffff);
                gen.writeRaw(c1);
                gen.writeRaw(c2);
                gen.writeRaw(value);
            } else {
                gen.writeRaw('\'');
                gen.writeRaw(value);
                gen.writeRaw('\'');
            }
        } else {
            gen.writeString(value);
        }
    }

    public static void serialize0(String value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        serialize2(value, gen, serializers);
    }

    public static void serializeKey(String value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        ((MsJsonGenerator) gen).verify();
        //可能有特殊字符的字段，用 !长度XXX 的方式进行序列化
        gen.writeRaw('!');
        int length = value.length();
        char c1 = (char) ((length >> 16) | 0x8000);
        char c2 = (char) (length & 0xffff);
        gen.writeRaw(c1);
        gen.writeRaw(c2);
        gen.writeRaw(value);
    }

    public static void serialize2(String value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (gen instanceof MsJsonGenerator) {
            ((MsJsonGenerator) gen).verify();
            //可能有特殊字符的字段，用 !长度XXX 的方式进行序列化
            if (value.length() > 2 && value.charAt(0) == '{') {
                gen.writeRaw('@');
                int length = value.length();
                char c1 = (char) ((length >> 15) | 0x8000);
                char c2 = (char) (length & 0x7fff);
                gen.writeRaw(c1);
                gen.writeRaw(c2);
                gen.writeRaw(value);
            } else {
                gen.writeRaw('\'');
                gen.writeRaw(value);
                gen.writeRaw('\'');
            }
        } else {
            gen.writeString(value);
        }
    }

    public static class StringSerializer extends JsonSerializer<String> {

        @Override
        public void serialize(String value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            serialize0(value, gen, serializers);
        }
    }

    public static class StringDeserializer extends JsonDeserializer<String> {

        @Override
        public String deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            if (p instanceof MsReaderBasedJsonParser) {
                return ((MsReaderBasedJsonParser) p).getText0();
            } else {
                return p.getText();
            }
        }
    }
}
