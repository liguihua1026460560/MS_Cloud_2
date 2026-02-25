package com.macrosan.message.jsonmsg.fast;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.macrosan.message.jsonmsg.FileMeta;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import io.vertx.core.json.EncodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;

import java.time.Instant;

@Log4j2
public class MsObjectMapper extends ObjectMapper {
    public static JsonDeserializer<Object> PartInfoArrayJsonDeserializer = null;

    public MsObjectMapper() {
        super(new MsMappingJsonFactory());
        _jsonFactory.setCodec(this);
        configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        SimpleModule module = new SimpleModule();
        module.addSerializer(JsonObject.class, new JsonSerializers.JsonObjectSerializer());
        module.addSerializer(JsonArray.class, new JsonSerializers.JsonArraySerializer());
        module.addSerializer(Instant.class, new JsonSerializers.InstantSerializer());
        module.addDeserializer(Instant.class, new JsonSerializers.InstantDeserializer());
        module.addSerializer(byte[].class, new JsonSerializers.ByteArraySerializer());
        module.addDeserializer(byte[].class, new JsonSerializers.ByteArrayDeserializer());

//        module.addSerializer(String.class, new MsStringJson.StringSerializer());
        module.addDeserializer(String.class, new MsStringJson.StringDeserializer());

        module.addDeserializer(MetaData.class, new MetaDataDeserializer());
        module.addSerializer(MetaData.class, new MetaDataSerializer());

        module.addDeserializer(FileMeta.class, new FileMetaDeserializer());
        module.addSerializer(FileMeta.class, new FileMetaSerializer());

        module.addDeserializer(UnSynchronizedRecord.class, new UnsyncRecordDeserializer());
        module.addSerializer(UnSynchronizedRecord.class, new UnsyncRecordSerializer());

        registerModule(module);

        JavaType type = _typeFactory.constructType(PartInfo[].class);
        if (PartInfoArrayJsonDeserializer == null) {
            try {
                PartInfoArrayJsonDeserializer = _deserializationContext.createInstance(_deserializationConfig, null, null).findRootValueDeserializer(type);
            } catch (JsonMappingException e) {
                log.error("", e);
            }
        }
    }

    private MsObjectMapper(boolean simplify) {
        super(new MsMappingJsonFactory());
        _jsonFactory.setCodec(this);
        configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        SimpleModule module = new SimpleModule();
        module.addSerializer(JsonObject.class, new JsonSerializers.JsonObjectSerializer());
        module.addSerializer(JsonArray.class, new JsonSerializers.JsonArraySerializer());
        module.addSerializer(Instant.class, new JsonSerializers.InstantSerializer());
        module.addDeserializer(Instant.class, new JsonSerializers.InstantDeserializer());
        module.addSerializer(byte[].class, new JsonSerializers.ByteArraySerializer());
        module.addDeserializer(byte[].class, new JsonSerializers.ByteArrayDeserializer());

//        module.addSerializer(String.class, new MsStringJson.StringSerializer());
        module.addDeserializer(String.class, new MsStringJson.StringDeserializer());

        module.addDeserializer(MetaData.class, new MetaDataDeserializer());
        module.addSerializer(MetaData.class, new SimplifyMetaDataSerializer());

        module.addDeserializer(FileMeta.class, new FileMetaDeserializer());
        module.addSerializer(FileMeta.class, new FileMetaSerializer());

        registerModule(module);
    }

    public static MsObjectMapper simplifyMapper = new MsObjectMapper(true);

    public static byte[] simplifyMetaJson(MetaData metaData) {
        try {
            return simplifyMapper.writeValueAsBytes(metaData);
        } catch (JsonProcessingException e) {
            throw new EncodeException("Failed to encode as JSON: " + e.getMessage());
        }
    }
}
