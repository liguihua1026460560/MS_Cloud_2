package com.macrosan.message.jsonmsg.fast;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.macrosan.message.jsonmsg.MetaData;

import java.io.IOException;

public class MetaDataSerializer extends JsonSerializer<MetaData> {
    @Override
    public void serialize(MetaData value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject(value);
        String sysMetaData = value.getSysMetaData();
        if (sysMetaData != null) {
            gen.writeFieldName("0");
            MsStringJson.serialize0(sysMetaData, gen, serializers);
        }
        String userMetaData = value.getUserMetaData();
        if (userMetaData != null &&!"{}".equals(userMetaData)) {
            gen.writeFieldName("1");
            MsStringJson.serialize0(userMetaData, gen, serializers);
        }
        String objectAcl = value.getObjectAcl();
        if (objectAcl != null) {
            gen.writeFieldName("2");
            MsStringJson.serialize0(objectAcl, gen, serializers);
        }
        String fileName = value.getFileName();
        if (fileName != null) {
            gen.writeFieldName("3");
            MsStringJson.serialize0(fileName, gen, serializers);
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
        String partUploadId = value.getPartUploadId();
        if (partUploadId != null) {
            gen.writeFieldName("partUploadId");
            MsStringJson.serialize0(partUploadId, gen, serializers);
        }
        Object partInfos = value.getPartInfos();
        if (partInfos != null) {
            gen.writeFieldName("partInfos");
            gen.writeObject(partInfos);
        }
        boolean smallFile = value.isSmallFile();
        if (smallFile) {
            gen.writeFieldName("smallFile");
            gen.writeBoolean(smallFile);
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
        String referencedBucket = value.getReferencedBucket();
        if (referencedBucket != null) {
            gen.writeFieldName("referencedBucket");
            MsStringJson.serialize0(referencedBucket, gen, serializers);
        }
        String referencedKey = value.getReferencedKey();
        if (referencedKey != null) {
            gen.writeFieldName("referencedKey");
            MsStringJson.serializeKey(referencedKey, gen, serializers);
        }
        String referencedVersionId = value.getReferencedVersionId();
        if (!"null".equals(referencedVersionId)) {
            gen.writeFieldName("referencedVersionId");
            MsStringJson.serialize0(referencedVersionId, gen, serializers);
        }
        boolean latest = value.isLatest();
        if (latest) {
            gen.writeFieldName("latest");
            gen.writeBoolean(latest);
        }
        String storage = value.getStorage();
        if (!"".equals(storage)) {
            gen.writeFieldName("8");
            MsStringJson.serialize0(storage, gen, serializers);
        }
        long size = value.getSize();
        if (size != 0) {
            gen.writeFieldName("size");
            gen.writeNumber(size);
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
        Object strategySet = value.getStrategySet();
        if (strategySet != null) {
            gen.writeFieldName("strategySet");
            gen.writeObject(strategySet);
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
        String aggregationKey = value.getAggregationKey();
        if (aggregationKey != null) {
            gen.writeFieldName("aggregationKey");
            MsStringJson.serialize0(aggregationKey, gen, serializers);
        }
        long offset = value.getOffset();
        if (offset != 0) {
            gen.writeFieldName("offset");
            gen.writeNumber(offset);
        }
        long aggSize = value.getAggSize();
        if (aggSize != 0) {
            gen.writeFieldName("aggSize");
            gen.writeNumber(aggSize);
        }
        String lastAccessStamp = value.getLastAccessStamp();
        if (lastAccessStamp != null) {
            gen.writeFieldName("lastAccessStamp");
            MsStringJson.serialize0(lastAccessStamp, gen, serializers);
        }

        gen.writeEndObject();
    }
}
