package com.macrosan.message.jsonmsg.fast;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonTokenId;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.macrosan.message.jsonmsg.UnSynchronizedRecord;
import io.vertx.core.http.HttpMethod;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class UnsyncRecordDeserializer extends JsonDeserializer<UnSynchronizedRecord> {
    @Override
    public UnSynchronizedRecord deserialize(JsonParser p, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
        if (p instanceof MsReaderBasedJsonParser) {
            MsReaderBasedJsonParser parser = ((MsReaderBasedJsonParser) p);
            UnSynchronizedRecord record = new UnSynchronizedRecord();
            p.nextToken();
            if (p.hasTokenId(JsonTokenId.ID_FIELD_NAME)) {
                String propName = p.getCurrentName();
                do {
                    p.nextToken();
                    switch (propName) {
                        case "1":
                        case "index":
                            record.setIndex(parser.getIntValue());
                            break;
                        case "2":
                        case "successIndex":
                            record.setSuccessIndex(parser.getIntValue());
                            break;
                        case "3":
                        case "headers":
                            Map<String, String> headers = parser.readValueAs(new TypeReference<Map<String, String>>() {});
                            record.setHeaders(headers);
                            break;
                        case "4":
                        case "syncStamp":
                            record.setSyncStamp(parser.getText0());
                            break;
                        case "versionNum":
                            record.setVersionNum(parser.getText0());
                            break;
                        case "5":
                        case "deleteMark":
                            record.setDeleteMark(parser.getBooleanValue());
                            break;
                        case "6":
                        case "uri":
                            record.setUri(parser.getText0());
                            break;
                        case "7":
                        case "method":
                            record.setMethod(HttpMethod.valueOf(parser.getText0()));
                            break;
                        case "8":
                        case "bucket":
                            record.setBucket(parser.getText0());
                            break;
                        case "9":
                        case "object":
                            record.setObject(parser.getText0());
                            break;
                        case "a":
                        case "versionId":
                            record.setVersionId(parser.getText0());
                            break;
                        case "b":
                        case "successIndexSet":
                            record.setSuccessIndexSet(parser.readValueAs(new TypeReference<Set<Integer>>() {
                            }));
                            break;
                        case "c":
                        case "commited":
                            record.setCommited(parser.getBooleanValue());
                            break;
                        case "d":
                        case "syncFlag":
                            record.setSyncFlag(parser.getBooleanValue());
                            break;
                        case "e":
                        case "recordKey":
                            record.setRecordKey(parser.getText0());
                            break;
                        case "f":
                        case "lastStamp":
                            record.setLastStamp(parser.getText0());
                            break;
                    }
                } while ((propName = p.nextFieldName()) != null);
            }
            return record;
        } else {
            throw new UnsupportedOperationException();
        }
    }
}
