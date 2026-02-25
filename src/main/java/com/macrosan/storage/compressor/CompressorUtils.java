package com.macrosan.storage.compressor;

import com.macrosan.message.jsonmsg.FileMeta;
import com.macrosan.utils.functional.Tuple2;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.Arrays;

import static com.macrosan.storage.compressor.CompressorFactory.getCompressor;

/**
 * @author wanhao
 * @description: 数据压缩工具类
 * @date 2022/9/2 下午 2:13
 */
public class CompressorUtils {
    public static byte[] compressData(byte[] data, String compressorName) throws IOException {
        Compressor compressor = getCompressor(compressorName);
        return compressor.compress(data);
    }

    public static byte[] uncompressData(byte[] data, String compressorName) throws IOException {
        Compressor compressor = getCompressor(compressorName);
        return compressor.uncompress(data);
    }

    public static Tuple2<byte[], Boolean> compressDataCheckRatio(byte[] data, String compressorName, double compressRatio) throws IOException {
        Compressor compressor = getCompressor(compressorName);
        byte[] res = compressor.compress(data);
        if (res.length >= data.length) {
            return new Tuple2<>(data, false);
        }
        return new Tuple2<>(res, true);
    }

    public static double calculateCompressRatio(double dataLen, double resLen) {
        double ratio = 0.00;
        if (dataLen != 0) {
            DecimalFormat decimalFormat = new DecimalFormat("#0.0000");
            decimalFormat.setRoundingMode(RoundingMode.HALF_UP);
            String ratioStr = decimalFormat.format(resLen / dataLen);
            ratio = Double.parseDouble(ratioStr);
        }
        return ratio;
    }

    public static boolean checkCompressEnable(String compressName) {
        return compressName != null && !"none".equals(compressName);
    }

    public static void clearCompressInfo(JsonObject obj) {
        obj.remove("compression");
        obj.remove("compressBeforeLen");
        obj.remove("compressAfterLen");
        obj.remove("compressState");
    }

    public static void updateCompressStateIfNull(FileMeta fileMeta) {
        if (checkCompressEnable(fileMeta.getCompression()) && fileMeta.getCompressState() == null) {
            long[] compressState = new long[fileMeta.getCompressAfterLen().length];
            Arrays.fill(compressState, 1);
            fileMeta.setCompressState(compressState);
        }
    }
}
