package com.macrosan.utils.aggregation;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.AggregateFileMetadata;
import com.macrosan.utils.functional.Tuple3;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.LongStream;

import static com.macrosan.constants.SysConstants.*;

@Log4j2
public class AggregationUtils {

    public static char convertNumberToLetter(char number) {
        if (number < '0' || number > '9') {
            throw new IllegalArgumentException("Number must be between 0 and 9");
        }
        return (char) ('A' + (number - '0'));
    }

    public static int convertLetterToNumber(char letter) {
        if (letter < 'A' || letter > 'Z') {
            throw new IllegalArgumentException("Letter must be between A and Z");
        }
        return (char) ('0' + (letter - 'A'));
    }

    public static String generateAggregationId() {
        return UUID.randomUUID() + "-" + ServerConfig.getInstance().getHostUuid();
    }

    // 将BitSet序列化为Base64字符串
    public static String serialize(BitSet bitSet) {
        byte[] bytes = bitSet.toByteArray();
        return Base64.getEncoder().encodeToString(bytes);
    }

    // 反序列化Base64字符串为BitSet
    public static BitSet deserialize(String base64Str) {
        byte[] bytes = Base64.getDecoder().decode(base64Str);
        return BitSet.valueOf(bytes);
    }

    public static String toBinaryString(BitSet bitSet) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bitSet.length(); i++) {
            sb.append(bitSet.get(i) ? '1' : '0');
        }
        return sb.toString();
    }

    public static double calculateHoleRatio(AggregateFileMetadata meta, BitSet bitSet) {
        try {
            long totalHoleSize = 0;
            for (int i = 0; i < meta.getDeltaOffsets().length; i++) {
                if (!bitSet.get(i)) {
                    long len = i == meta.getDeltaOffsets().length - 1 ? meta.getFileSize() - meta.getDeltaOffsets()[i] : meta.getDeltaOffsets()[i + 1] - meta.getDeltaOffsets()[i];
                    totalHoleSize += len;
                }
            }
            return totalHoleSize / (double) meta.fileSize;
        } catch (Exception e) {
            log.error("", e);
            return 0;
        }
    }

    public static Tuple3<String, String, String> getInfoFromAggregationKey(String key) {
        // ?15972/SS_strategy_B/04c94dd5-3919-432a-aeca-7336dce07d4f-0001
        String vnode = key.split("/")[0].substring(1);
        String nameSpace = key.split("/")[1];
        String aggregationId = key.split("/")[2];
        return new Tuple3<>(vnode, nameSpace, aggregationId);
    }

    public static String getAggregationKey(String vnode, String nameSpace, String aggregationId) {
        return ROCKS_AGGREGATION_META_PREFIX + vnode + File.separator + nameSpace + File.separator + aggregationId;
    }

    // 聚合文件位图键
    public static String getAggregateFileBitmapKey(String vnode, String nameSpace, String aggregationId) {
        return ROCKS_AGGREGATION_RATE_PREFIX + vnode + File.separator + nameSpace + File.separator + aggregationId;
    }

    // 聚合文件GC键，用于对聚合文件的GC
    public static String getAggregateGcKey(String vnode, String nameSpace, String aggregationId) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < nameSpace.length(); i++) {
            char c = nameSpace.charAt(i);
            if (c >= '0' && c <= '9') {
                c = convertNumberToLetter(c);
            }
            sb.append(c);
        }
        return ROCKS_OBJ_META_DELETE_MARKER + sb +File.separator + vnode + File.separator + aggregationId;
    }

    public static String convertNameSpace(String nameSpace) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < nameSpace.length(); i++) {
            char c = nameSpace.charAt(i);
            if (c >= '0' && c <= '9') {
                c = convertNumberToLetter(c);
            }
            sb.append(c);
        }
        return sb.toString();
    }
}
