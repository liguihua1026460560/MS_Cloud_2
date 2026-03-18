package com.macrosan.message.jsonmsg.fast;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonTokenId;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.std.PrimitiveArrayDeserializers;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;

import java.io.IOException;
import java.util.Set;

import static com.macrosan.message.jsonmsg.fast.MsObjectMapper.PartInfoArrayJsonDeserializer;

public class MetaDataDeserializer extends JsonDeserializer<MetaData> {
    private static JsonDeserializer<long[]> LongArrayJsonDeserializer = (JsonDeserializer<long[]>) PrimitiveArrayDeserializers.forType(Long.TYPE);

    @Override
    public MetaData deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        if (p instanceof MsReaderBasedJsonParser) {
            MsReaderBasedJsonParser parser = ((MsReaderBasedJsonParser) p);
            MetaData metadata = new MetaData();
            p.nextToken();
            if (p.hasTokenId(JsonTokenId.ID_FIELD_NAME)) {
                String propName = p.getCurrentName();
                do {
                    p.nextToken();
                    switch (propName) {
                        case "0":
                        case "sysMetaData":
                            metadata.setSysMetaData(parser.getText0());
                            break;
                        case "1":
                        case "userMetaData":
                            metadata.setUserMetaData(parser.getText0());
                            break;
                        case "2":
                        case "objectAcl":
                            metadata.setObjectAcl(parser.getText0());
                            break;
                        case "3":
                        case "fileName":
                            metadata.setFileName(parser.getText0());
                            break;
                        case "startIndex":
                            metadata.setStartIndex(parser.getLongValue());
                            break;
                        case "4":
                        case "endIndex":
                            metadata.setEndIndex(parser.getLongValue());
                            break;
                        case "versionNum":
                            metadata.setVersionNum(parser.getText0());
                            break;
                        case "6":
                        case "syncStamp":
                            metadata.setSyncStamp(parser.getText0());
                            break;
                        case "7":
                        case "shardingStamp":
                            metadata.setShardingStamp(parser.getText0());
                            break;
                        case "deleteMark":
                            metadata.setDeleteMark(parser.getBooleanValue());
                            break;
                        case "partUploadId":
                            metadata.setPartUploadId(parser.getText0());
                            break;
                        case "partInfos":
                            metadata.setPartInfos((PartInfo[]) PartInfoArrayJsonDeserializer.deserialize(p, ctxt));
                            break;
                        case "smallFile":
                            metadata.setSmallFile(parser.getBooleanValue());
                            break;
                        case "versionId":
                            metadata.setVersionId(parser.getText0());
                            break;
                        case "stamp":
                            metadata.setStamp(parser.getText0());
                            break;
                        case "deleteMarker":
                            metadata.setDeleteMarker(parser.getBooleanValue());
                            break;
                        case "referencedBucket":
                            metadata.setReferencedBucket(parser.getText0());
                            break;
                        case "referencedKey":
                            metadata.setReferencedKey(parser.getText0());
                            break;
                        case "referencedVersionId":
                            metadata.setReferencedVersionId(parser.getText0());
                            break;
                        case "latest":
                            metadata.setLatest(parser.getBooleanValue());
                            break;
                        case "8":
                        case "storage":
                            metadata.setStorage(parser.getText0());
                            break;
                        case "size":
                            metadata.setSize(parser.getLongValue());
                            break;
                        case "inode":
                            metadata.setInode(parser.getLongValue());
                            break;
                        case "tmpInodeStr":
                            metadata.setTmpInodeStr(parser.getText0());
                            break;
                        case "cookie":
                            metadata.setCookie(parser.getLongValue());
                            break;
                        case "9":
                        case "key":
                            metadata.setKey(parser.getText0());
                            break;
                        case "a":
                        case "bucket":
                            metadata.setBucket(parser.getText0());
                            break;
                        case "duplicateKey":
                            metadata.setDuplicateKey(parser.getText0());
                            break;
                        case "crypto":
                            metadata.setCrypto(parser.getText0());
                            break;
                        case "discard":
                            metadata.setDiscard(parser.getBooleanValue());
                            break;
                        case "strategySet":
                            metadata.setStrategySet((Set<String>) parser.getCurrentValue());
                            break;
                        case "snapshotMark":
                            metadata.setSnapshotMark(parser.getText0());
                            break;
                        case "unView":
                            metadata.setUnView((Set<String>) parser.getCurrentValue());
                            break;
                        case "weakUnView":
                            metadata.setWeakUnView((Set<String>) parser.getCurrentValue());
                            break;
                        case "aggregationKey":
                            metadata.setAggregationKey(parser.getText0());
                            break;
                        case "offset":
                            metadata.setOffset(parser.getLongValue());
                            break;
                        case "aggSize":
                            metadata.setAggSize(parser.getLongValue());
                            break;
                        case "lastAccessStamp":
                            metadata.setLastAccessStamp(parser.getText0());
                            break;
                    }
                } while ((propName = p.nextFieldName()) != null);
            }
            if (metadata.referencedKey == null){
                metadata.setReferencedKey(metadata.key);
            }
            if (metadata.referencedBucket == null){
                metadata.setReferencedBucket(metadata.bucket);
            }
            if (metadata.referencedVersionId == null){
                metadata.setReferencedVersionId("null");
            }
            return metadata;
        } else {
            throw new UnsupportedOperationException();
        }
    }
}
