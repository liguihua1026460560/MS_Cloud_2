package com.macrosan.message.jsonmsg.fast;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonTokenId;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.std.PrimitiveArrayDeserializers;
import com.macrosan.message.jsonmsg.FileMeta;

import java.io.IOException;

public class FileMetaDeserializer extends JsonDeserializer<FileMeta> {
    private static JsonDeserializer<long[]> LongArrayJsonDeserializer = (JsonDeserializer<long[]>) PrimitiveArrayDeserializers.forType(Long.TYPE);

    @Override
    public FileMeta deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        if (p instanceof MsReaderBasedJsonParser) {
            MsReaderBasedJsonParser parser = ((MsReaderBasedJsonParser) p);
            FileMeta filemeta = new FileMeta();
            p.nextToken();
            if (p.hasTokenId(JsonTokenId.ID_FIELD_NAME)) {
                String propName = p.getCurrentName();
                do {
                    p.nextToken();
                    switch (propName) {
                        case "1":
                        case "lun":
                            filemeta.setLun(parser.getText0());
                            break;
                        case "2":
                        case "fileName":
                            filemeta.setFileName(parser.getText0());
                            break;
                        case "3":
                        case "etag":
                            filemeta.setEtag(parser.getText0());
                            break;
                        case "4":
                        case "size":
                            filemeta.setSize(parser.getLongValue());
                            break;
                        case "smallFile":
                            filemeta.setSmallFile(parser.getBooleanValue());
                            break;
                        case "5":
                        case "offset":
                            filemeta.setOffset(LongArrayJsonDeserializer.deserialize(p, ctxt));
                            break;
                        case "6":
                        case "len":
                            filemeta.setLen(LongArrayJsonDeserializer.deserialize(p, ctxt));
                            break;
                        case "7":
                        case "metaKey":
                            filemeta.setMetaKey(parser.getText0());
                            break;
                        case "compression":
                            filemeta.setCompression(parser.getText0());
                            break;
                        case "compressBeforeLen":
                            filemeta.setCompressBeforeLen(LongArrayJsonDeserializer.deserialize(p, ctxt));
                            break;
                        case "compressAfterLen":
                            filemeta.setCompressAfterLen(LongArrayJsonDeserializer.deserialize(p, ctxt));
                            break;
                        case "compressState":
                            filemeta.setCompressState(LongArrayJsonDeserializer.deserialize(p, ctxt));
                            break;
                        case "crypto":
                            filemeta.setCrypto(parser.getText0());
                            break;
                        case "secretKey":
                            filemeta.setSecretKey(parser.getText0());
                            break;
                        case "cryptoBeforeLen":
                            filemeta.setCryptoBeforeLen(LongArrayJsonDeserializer.deserialize(p, ctxt));
                            break;
                        case "cryptoAfterLen":
                            filemeta.setCryptoAfterLen(LongArrayJsonDeserializer.deserialize(p, ctxt));
                            break;
                        case "cryptoVersion":
                            filemeta.setCryptoVersion(parser.getText0());
                            break;
                        case "8":
                        case "fileOffset":
                            filemeta.setFileOffset(parser.getLongValue());
                            break;
                        case "flushStamp":
                            filemeta.setFlushStamp(parser.getText0());
                            break;
                        case "lastAccessStamp":
                            filemeta.setLastAccessStamp(parser.getText0());
                            break;
                    }
                } while ((propName = p.nextFieldName()) != null);
            }
            return filemeta;
        } else {
            throw new UnsupportedOperationException();
        }
    }
}
