package com.macrosan.message.jsonmsg.fast;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import io.vertx.core.http.HttpMethod;

import java.io.IOException;
import java.util.Set;

public class UnsyncRecordSerializer extends JsonSerializer<UnSynchronizedRecord> {
    @Override
    public void serialize(UnSynchronizedRecord value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject(value);

        if (value.index != null) {
            gen.writeFieldName("1");
            gen.writeNumber(value.index);
        }
        if (value.successIndex != null) {
            gen.writeFieldName("2");
            gen.writeNumber(value.successIndex);
        }
        if (value.getHeaders() != null) {
            Object headers = value.getHeaders();
            gen.writeFieldName("3");
            gen.writeObject(headers);
        }
        String versionNum = value.getVersionNum();
        if (versionNum != null) {
            gen.writeFieldName("versionNum");
            MsStringJson.serialize0(versionNum, gen, serializers);
        }
        String syncStamp = value.syncStamp;
        if (syncStamp != null) {
            gen.writeFieldName("4");
            MsStringJson.serialize0(syncStamp, gen, serializers);
        }
        boolean deleteMark = value.deleteMark;
        if (deleteMark) {
            gen.writeFieldName("5");
            gen.writeBoolean(deleteMark);
        }
        String uri = value.uri;
        if (uri != null) {
            gen.writeFieldName("6");
            MsStringJson.serialize0(uri, gen, serializers);
        }
        HttpMethod method = value.method;
        if (method != null) {
            gen.writeFieldName("7");
            String methodStr = method.name();
            MsStringJson.serialize0(methodStr, gen, serializers);
        }
        String bucket = value.bucket;
        if (bucket != null) {
            gen.writeFieldName("8");
            MsStringJson.serialize0(bucket, gen, serializers);
        }
        String object = value.object;
        if (object != null) {
            gen.writeFieldName("9");
            MsStringJson.serialize0(object, gen, serializers);
        }
        String versionId = value.versionId;
        if (versionId != null) {
            gen.writeFieldName("a");
            MsStringJson.serialize0(versionId, gen, serializers);
        }
        Set<Integer> successIndexSet = value.successIndexSet;
        if (successIndexSet != null) {
            gen.writeFieldName("b");
            gen.writeObject(successIndexSet);
        }
        boolean commited = value.commited;
        if (commited) {
            gen.writeFieldName("c");
            gen.writeBoolean(commited);
        }
        boolean syncFlag = value.syncFlag;
        if (syncFlag) {
            gen.writeFieldName("d");
            gen.writeBoolean(syncFlag);
        }
        String recordKey = value.recordKey;
        if (recordKey != null) {
            gen.writeFieldName("e");
            MsStringJson.serialize0(recordKey, gen, serializers);
        }
        String lastStamp = value.lastStamp;
        if (lastStamp != null) {
            gen.writeFieldName("f");
            MsStringJson.serialize0(lastStamp, gen, serializers);
        }

        gen.writeEndObject();
    }
}
