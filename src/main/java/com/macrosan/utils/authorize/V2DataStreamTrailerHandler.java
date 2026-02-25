package com.macrosan.utils.authorize;

import com.macrosan.constants.ErrorNo;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import lombok.Getter;
import org.apache.commons.codec.DecoderException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.zip.CRC32;
import static com.macrosan.utils.authorize.AuthorizeV4.X_AMZ_TRAILER;
import static com.macrosan.utils.authorize.AuthorizeV4.X_AMZ_TRAILER_ALGORITHM;

public class V2DataStreamTrailerHandler implements Handler<Buffer>{
    private static final Logger logger = LogManager.getLogger(V2DataStreamTrailerHandler.class);
    private static final int HEX = 16;
    private static final int PART = 1024;
    //块长度最大10字节  即块的最大长度 2^40 - 1
    private static final int MAX_CHUNK_SIZE_BUFFER = 10;
    /**
     * handler 状态
     * 0：读数据块长度
     * 1：读数据
     * 2：读数据块结束符\r
     * 3：读数据块结束符\n
     * 4：读取尾随块
     */
    private int status = STATUS_CHUNK_LENGTH;
    private static final int STATUS_CHUNK_LENGTH = 0;
    private static final int STATUS_CHUNK_DATA = 1;
    private static final int STATUS_CHUNK_SUFFIX_R = 2;
    private static final int STATUS_CHUNK_SUFFIX_N = 3;
    private static final int STATUS_CHUNK_TRAILER = 4;

    private Consumer<byte[]> bytesConsumer;
    private Consumer<Throwable> errorConsumer;
    private MsHttpRequest request;
    @Getter
    private MessageDigest sha256;
    private CRC32 crc32 = new CRC32();
    private Buffer currentParsingBuffer;
    private int currentBufferReadLength = 0;
    private int currentChunkTotalLength = -1;
    private int currentChunkReadLength = 0;
    private int chunkLengthRead = 0;
    private byte[] chunkLengthBytes = new byte[MAX_CHUNK_SIZE_BUFFER];
    private int trailerChunkRead = 0;
    private byte[] trailerChunkBytes = new byte[PART];
    private String[] trailerChunkArray;


    public V2DataStreamTrailerHandler(MsHttpRequest request, Consumer<byte[]> bytesConsumer, Consumer<Throwable> errorConsumer) {
        this.request = request;
        LinkedList<byte[]> data = new LinkedList<>();
        this.bytesConsumer = b -> {
            if (b.length > 0) {
                data.add(b);
            } else {
                byte[] res;
                if (data.size() == 1) {
                    res = data.pollFirst();
                } else if (data.size() == 0) {
                    res = b;
                } else {
                    int n = data.stream().mapToInt(b0 -> b0.length).sum();
                    res = new byte[n];
                    int i = 0;
                    for (byte[] bytes : data) {
                        System.arraycopy(bytes, 0, res, i, bytes.length);
                        i += bytes.length;
                    }

                    data.clear();
                }
                bytesConsumer.accept(res);
            }
        };

        this.errorConsumer = errorConsumer;
        try {
            sha256 = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            Optional.ofNullable(errorConsumer).ifPresent(consumer -> consumer.accept(e));
            e.printStackTrace();
        }
    }

    public void handle(Buffer event) {
        try {
            parse(event);
        } catch (Exception e) {
            Optional.ofNullable(errorConsumer).ifPresent(consumer -> consumer.accept(e));
        }
    }

    /**
     * 解析数据
     */
    private void parse(Buffer buffer) throws DecoderException {
        currentParsingBuffer = buffer;
        currentBufferReadLength = 0;
        boolean parsing = true;

        while (parsing) {
            switch (status) {
                case STATUS_CHUNK_LENGTH:
                    parsing = parseLength();
                    break;
                case STATUS_CHUNK_DATA:
                    parsing = parseData();
                    break;
                case STATUS_CHUNK_SUFFIX_R:
                    parsing = parseSuffixR();
                    break;
                case STATUS_CHUNK_SUFFIX_N:
                    parsing = parseSuffixN();
                    break;
                case STATUS_CHUNK_TRAILER:
                    parsing = parseTrailer();
                    break;
                default:
                    throw new MsException(ErrorNo.INCOMPLETE_BODY, "V2DataStreamTrailerHandler state error");
            }
        }

        bytesConsumer.accept(new byte[0]);
    }

    private boolean parseLength() {
        //读取当前块长度
        for (; chunkLengthRead < MAX_CHUNK_SIZE_BUFFER && currentBufferReadLength < currentParsingBuffer.length(); chunkLengthRead++, currentBufferReadLength++) {
            byte b = currentParsingBuffer.getByte(currentBufferReadLength);
            if ('\n' == b && '\r' == chunkLengthBytes[chunkLengthRead - 1]) {
                String s = new String(chunkLengthBytes, 0, chunkLengthRead).replaceAll("\\s+", "");
                currentChunkTotalLength = Integer.parseInt(s, HEX);
                currentBufferReadLength++;
                sha256.reset();
                if (currentChunkTotalLength == 0){
                    status = STATUS_CHUNK_TRAILER;
                }else {
                    status = STATUS_CHUNK_DATA;
                }
                return bufferNotFinished();
            }
            if (!((b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || (b >= 'A' && b <= 'Z') || ('\r' == b))) {
                //非法字符
                throw new MsException(ErrorNo.INCOMPLETE_BODY, "the length chunk includes error char");
            }
            chunkLengthBytes[chunkLengthRead] = b;
        }
        if (chunkLengthRead >= MAX_CHUNK_SIZE_BUFFER) {
            //块长度过长
            throw new MsException(ErrorNo.INCOMPLETE_BODY, "the length chunk is too long");
        }
        return bufferNotFinished();
    }


    private boolean parseData() {
        int readLen = Math.min(currentParsingBuffer.length() - currentBufferReadLength, currentChunkTotalLength - currentChunkReadLength);
        if (readLen > 0) {
            byte[] bytes = currentParsingBuffer.getBytes(currentBufferReadLength, currentBufferReadLength + readLen);
            sha256.update(bytes);
            crc32.update(bytes);
            Optional.ofNullable(bytesConsumer).ifPresent(consumer -> consumer.accept(bytes));
        }
        currentBufferReadLength += readLen;
        currentChunkReadLength += readLen;
        if (currentChunkReadLength >= currentChunkTotalLength) {
            status = STATUS_CHUNK_SUFFIX_R;
        }
        return bufferNotFinished();
    }

    private boolean parseSuffixR() {
        if ('\r' == currentParsingBuffer.getByte(currentBufferReadLength)) {
            status = STATUS_CHUNK_SUFFIX_N;
            currentBufferReadLength++;
        } else {
            throw new MsException(ErrorNo.INCOMPLETE_BODY, "the char is not r");
        }
        return bufferNotFinished();

    }

    private boolean parseSuffixN() {
        if ('\n' == currentParsingBuffer.getByte(currentBufferReadLength)) {
            status = STATUS_CHUNK_LENGTH;
            currentChunkReadLength = 0;
            currentChunkTotalLength = -1;
            chunkLengthRead = 0;
            currentBufferReadLength++;
        } else {
            throw new MsException(ErrorNo.INCOMPLETE_BODY, "the char is not n");
        }
        return bufferNotFinished();
    }

    private boolean parseTrailer() {
        //开始读取尾随块
        for (; trailerChunkRead < PART && currentBufferReadLength < currentParsingBuffer.length(); trailerChunkRead++, currentBufferReadLength++) {
            byte b = currentParsingBuffer.getByte(currentBufferReadLength);
            if ('\n' == b && '\r' == trailerChunkBytes[trailerChunkRead - 1]) {
                String trailerChunk = new String(trailerChunkBytes, 0, trailerChunkRead).replaceAll("\\s+", "");
                trailerChunkArray = trailerChunk.split(":");
                if (2 != trailerChunkArray.length) {
                    throw new MsException(ErrorNo.INCOMPLETE_BODY, "trailer chunk is error");
                }
                //检验头
                if (!(trailerChunkArray[0].trim().equals(request.getHeader(X_AMZ_TRAILER)) && X_AMZ_TRAILER_ALGORITHM.equals(trailerChunkArray[0].trim()))){
                    throw new MsException(ErrorNo.INCOMPLETE_BODY, "trailer chunk header is error");
                }
                //校验值
                String serverCrcValue = BigEndianCRC32.longToBigEndianBase64(crc32.getValue());
                if (!serverCrcValue.equals(trailerChunkArray[1])) {
                    throw new MsException(ErrorNo.INCOMPLETE_BODY, "CRC32 checksum is not match with body");
                }
                request.response().putHeader(X_AMZ_TRAILER_ALGORITHM,serverCrcValue);
                trailerChunkRead++;
                currentBufferReadLength++;
                return false;
            }
            if (!((b >= 'a' && b <= 'z')
                    || (b >= '0' && b <= '9')
                    || (b >= 'A' && b <= 'Z')
                    || '-' == b
                    || ':' == b
                    || '=' == b
                    || '\r' == b
                    || ' ' == b
                    || '\n' == b)) {
                //非法字符
                throw new MsException(ErrorNo.INCOMPLETE_BODY, "the trailer chunk includes error char");
            }
            trailerChunkBytes[trailerChunkRead] = b;
        }
        if (trailerChunkRead >= PART) {
            throw new MsException(ErrorNo.INCOMPLETE_BODY, "the trailer chunk is too long");
        }
        //直接结束
        return false;
    }

    private boolean bufferNotFinished() {
        //判断当前buffer读完
        return currentBufferReadLength < currentParsingBuffer.length();
    }

}
