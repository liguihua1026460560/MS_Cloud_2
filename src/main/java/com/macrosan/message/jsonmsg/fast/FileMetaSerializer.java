package com.macrosan.message.jsonmsg.fast;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.macrosan.message.jsonmsg.FileMeta;

import java.io.IOException;

public class FileMetaSerializer extends JsonSerializer<FileMeta> {
    @Override
    public void serialize(FileMeta value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject(value);
        String lun = value.getLun();
        if (lun != null) {
            gen.writeFieldName("1");
            MsStringJson.serialize0(lun, gen, serializers);
        }
        String fileName = value.getFileName();
        if (fileName != null) {
            gen.writeFieldName("2");
            MsStringJson.serialize0(fileName, gen, serializers);
        }
        String etag = value.getEtag();
        if (etag != null) {
            gen.writeFieldName("3");
            MsStringJson.serialize0(etag, gen, serializers);
        }
        long size = value.getSize();
        if (size != 0) {
            gen.writeFieldName("4");
            gen.writeNumber(size);
        }
        boolean smallFile = value.isSmallFile();
        if (smallFile) {
            gen.writeFieldName("smallFile");
            gen.writeBoolean(smallFile);
        }
        long[] offset = value.getOffset();
        if (offset != null) {
            gen.writeFieldName("5");
            gen.writeStartArray();
            for (int i = 0; i < offset.length; ++i) {
                gen.writeNumber(offset[i]);
            }
            gen.writeEndArray();
        }
        long[] len = value.getLen();
        if (len != null) {
            gen.writeFieldName("6");
            gen.writeStartArray();
            for (int i = 0; i < len.length; ++i) {
                gen.writeNumber(len[i]);
            }
            gen.writeEndArray();
        }
        String metaKey = value.getMetaKey();
        if (metaKey != null) {
            gen.writeFieldName("7");
            MsStringJson.serializeKey(metaKey, gen, serializers);
        }
        String compression = value.getCompression();
        if (compression != null) {
            gen.writeFieldName("compression");
            MsStringJson.serialize0(compression, gen, serializers);
        }
        long[] compressBeforeLen = value.getCompressBeforeLen();
        if (compressBeforeLen != null) {
            gen.writeFieldName("compressBeforeLen");
            gen.writeStartArray();
            for (int i = 0; i < compressBeforeLen.length; ++i) {
                gen.writeNumber(compressBeforeLen[i]);
            }
            gen.writeEndArray();
        }
        long[] compressAfterLen = value.getCompressAfterLen();
        if (compressAfterLen != null) {
            gen.writeFieldName("compressAfterLen");
            gen.writeStartArray();
            for (int i = 0; i < compressAfterLen.length; ++i) {
                gen.writeNumber(compressAfterLen[i]);
            }
            gen.writeEndArray();
        }
        long[] compressState = value.getCompressState();
        if (compressState != null) {
            gen.writeFieldName("compressState");
            gen.writeStartArray();
            for (int i = 0; i < compressState.length; ++i) {
                gen.writeNumber(compressState[i]);
            }
            gen.writeEndArray();
        }
        String crypto = value.getCrypto();
        if (crypto != null) {
            gen.writeFieldName("crypto");
            MsStringJson.serialize0(crypto, gen, serializers);
        }
        String secretKey = value.getSecretKey();
        if (secretKey != null) {
            gen.writeFieldName("secretKey");
            MsStringJson.serialize0(secretKey, gen, serializers);
        }
        long[] cryptoBeforeLen = value.getCryptoBeforeLen();
        if (cryptoBeforeLen != null) {
            gen.writeFieldName("cryptoBeforeLen");
            gen.writeStartArray();
            for (int i = 0; i < cryptoBeforeLen.length; ++i) {
                gen.writeNumber(cryptoBeforeLen[i]);
            }
            gen.writeEndArray();
        }
        long[] cryptoAfterLen = value.getCryptoAfterLen();
        if (cryptoAfterLen != null) {
            gen.writeFieldName("cryptoAfterLen");
            gen.writeStartArray();
            for (int i = 0; i < cryptoAfterLen.length; ++i) {
                gen.writeNumber(cryptoAfterLen[i]);
            }
            gen.writeEndArray();
        }
        String cryptoVersion = value.getCryptoVersion();
        if (cryptoVersion != null) {
            gen.writeFieldName("cryptoVersion");
            MsStringJson.serialize0(cryptoVersion, gen, serializers);
        }
        long fileOffset = value.getFileOffset();
        if (fileOffset != 0) {
            gen.writeFieldName("8");
            gen.writeNumber(fileOffset);
        }
        String flushStamp = value.getFlushStamp();
        if (flushStamp != null) {
            gen.writeFieldName("flushStamp");
            MsStringJson.serialize0(flushStamp, gen, serializers);
        }
        gen.writeEndObject();
    }
}
