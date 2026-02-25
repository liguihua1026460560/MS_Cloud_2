package com.macrosan.message.jsonmsg.fast;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.macrosan.ec.Utils;
import com.macrosan.message.jsonmsg.MetaData;

import java.io.IOException;

public class SimplifyMetaDataSerializer extends MetaDataSerializer {
    @Override
    public void serialize(MetaData value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject(value);

        String sysMetaData = value.getSysMetaData();
        if (sysMetaData != null) {
            gen.writeFieldName("0");
            MsStringJson.serialize0(Utils.simplifySysMeta(sysMetaData), gen, serializers);
        }


        long startIndex = value.getStartIndex();
        if (startIndex != 0) {
            gen.writeFieldName("startIndex");
            gen.writeNumber(startIndex);
        }
        long endIndex = value.getEndIndex();
        if (endIndex != 0) {
            gen.writeFieldName("4");
            gen.writeNumber(endIndex);
        }
        String versionNum = value.getVersionNum();
        if (versionNum != null) {
            gen.writeFieldName("versionNum");
            MsStringJson.serialize0(versionNum, gen, serializers);
        }
        String syncStamp = value.getSyncStamp();
        if (syncStamp != null) {
            gen.writeFieldName("6");
            MsStringJson.serialize0(syncStamp, gen, serializers);
        }
        String shardingStamp = value.getShardingStamp();
        if (shardingStamp != null) {
            gen.writeFieldName("7");
            MsStringJson.serialize0(shardingStamp, gen, serializers);
        }
        boolean deleteMark = value.isDeleteMark();
        if (deleteMark) {
            gen.writeFieldName("deleteMark");
            gen.writeBoolean(deleteMark);
        }


        String versionId = value.getVersionId();
        if (!"null".equals(versionId)) {
            gen.writeFieldName("versionId");
            MsStringJson.serialize0(versionId, gen, serializers);
        }
        String stamp = value.getStamp();
        if (stamp != null) {
            gen.writeFieldName("stamp");
            MsStringJson.serialize0(stamp, gen, serializers);
        }
        boolean deleteMarker = value.isDeleteMarker();
        if (deleteMarker) {
            gen.writeFieldName("deleteMarker");
            gen.writeBoolean(deleteMarker);
        }


        String storage = value.getStorage();
        if (!"".equals(storage)) {
            gen.writeFieldName("8");
            MsStringJson.serialize0(storage, gen, serializers);
        }

        long inode = value.getInode();
        if (inode != 0) {
            gen.writeFieldName("inode");
            gen.writeNumber(inode);
        }
        String tmpInodeStr = value.getTmpInodeStr();
        if (tmpInodeStr != null) {
            gen.writeFieldName("tmpInodeStr");
            MsStringJson.serialize0(tmpInodeStr, gen, serializers);
        }
        long cookie = value.getCookie();
        if (cookie != 0) {
            gen.writeFieldName("cookie");
            gen.writeNumber(cookie);
        }
        String key = value.getKey();
        if (key != null) {
            gen.writeFieldName("9");
            MsStringJson.serializeKey(key, gen, serializers);
        }
        String bucket = value.getBucket();
        if (bucket != null) {
            gen.writeFieldName("a");
            MsStringJson.serialize0(bucket, gen, serializers);
        }
        String duplicateKey = value.getDuplicateKey();
        if (duplicateKey != null) {
            gen.writeFieldName("duplicateKey");
            MsStringJson.serialize0(duplicateKey, gen, serializers);
        }
        String crypto = value.getCrypto();
        if (crypto != null) {
            gen.writeFieldName("crypto");
            MsStringJson.serialize0(crypto, gen, serializers);
        }
        boolean discard = value.isDiscard();
        if (discard) {
            gen.writeFieldName("discard");
            gen.writeBoolean(discard);
        }

        String snapshotMark = value.getSnapshotMark();
        if (snapshotMark != null) {
            gen.writeFieldName("snapshotMark");
            MsStringJson.serialize0(snapshotMark, gen, serializers);
        }
        Object unView = value.getUnView();
        if (unView != null) {
            gen.writeFieldName("unView");
            gen.writeObject(unView);
        }
        Object weakUnView = value.getWeakUnView();
        if (weakUnView != null) {
            gen.writeFieldName("weakUnView");
            gen.writeObject(weakUnView);
        }

        long offset = value.getOffset();
        if (offset != 0) {
            gen.writeFieldName("offset");
            gen.writeNumber(offset);
        }

        gen.writeEndObject();
    }
}
