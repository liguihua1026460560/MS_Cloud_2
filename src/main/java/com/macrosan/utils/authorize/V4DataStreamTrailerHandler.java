package com.macrosan.utils.authorize;

import com.macrosan.constants.ErrorNo;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import lombok.Getter;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.zip.CRC32;
import static com.macrosan.utils.authorize.AuthorizeV4.*;
public class V4DataStreamTrailerHandler implements Handler<Buffer> {
    private static final Logger logger = LogManager.getLogger(V4DataStreamTrailerHandler.class);
    private static final int HEX = 16;
    private static final int PART = 1024;
    //块长度最大10字节  即块的最大长度 2^40 - 1
    private static final int MAX_CHUNK_SIZE_BUFFER = 10;

    /**
     * handler 状态
     * 0：读数据块长度
     * 1：读数据块签名
     * 2：读数据
     * 3：读数据块结束符\r
     * 4：读数据块结束符\n
     * 5: 读数据块尾部头部
     * 6: 读数据块尾部值
     * 7: 读数据块尾部签名
     */
    private int status = STATUS_CHUNK_LENGTH;
    private static final int STATUS_CHUNK_LENGTH = 0;
    private static final int STATUS_CHUNK_SIGNATURE = 1;
    private static final int STATUS_CHUNK_DATA = 2;
    private static final int STATUS_CHUNK_SUFFIX_R = 3;
    private static final int STATUS_CHUNK_SUFFIX_N = 4;
    private static final int STATUS_CHUNK_TRAILER_HEADER = 5;
    private static final int STATUS_CHUNK_TRAILER_VALUE = 6;
    private static final int STATUS_CHUNK_TRAILER_SIGNATURE = 7;

    private Consumer<byte[]> bytesConsumer;
    private Consumer<Throwable> errorConsumer;
    private MsHttpRequest request;
    @Getter
    private MessageDigest sha256;
    private CRC32 crc32 = new CRC32();
    private String stringToSignNoHash;
    private String signatureKey;
    private Buffer currentParsingBuffer;
    private int currentBufferReadLength = 0;
    private int currentChunkTotalLength = -1;
    private int currentChunkReadLength = 0;
    private int chunkLengthRead = 0;
    private byte[] chunkLengthBytes = new byte[MAX_CHUNK_SIZE_BUFFER];
    private int chunkSignatureRead = 0;
    private byte[] chunkSignatureBytes = new byte[PART];
    private String zeroDataChunkSignature;
    private String lastChunkSignature;
    private int trailerHeaderRead = 0;
    private byte[] chunkTrailerHeaderBytes = new byte[PART];
    private String trailerChunkHead;
    private int trailerSignatureRead = 0;
    private byte[] trailerSignatureBytes = new byte[PART];
    private String trailerChunkValue;
    private int crcRead = 0;
    private byte[] crcCheckbytes = new byte[PART];
    private String[] chunkSignatureArray;

    public V4DataStreamTrailerHandler(MsHttpRequest request, Consumer<byte[]> bytesConsumer, Consumer<Throwable> errorConsumer) {
        this.request = request;
        lastChunkSignature = request.getMember("seed_signature");
        stringToSignNoHash = request.getMember("string_to_sign_no_hash");
        signatureKey = request.getMember("signature_key");
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
                case STATUS_CHUNK_SIGNATURE:
                    parsing = parseSignature();
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
                case STATUS_CHUNK_TRAILER_HEADER:
                    parsing = parseTrailerHeader();
                    break;
                case STATUS_CHUNK_TRAILER_VALUE:
                    parsing = parseTrailerValue();
                    break;
                case STATUS_CHUNK_TRAILER_SIGNATURE:
                    parsing = parseTrailerSignature();
                    break;
                default:
                    throw new MsException(ErrorNo.INCOMPLETE_BODY, "chunk data error");
            }
        }

        bytesConsumer.accept(new byte[0]);
    }

    private boolean parseLength() {
        //读取当前块长度
        for (; chunkLengthRead < MAX_CHUNK_SIZE_BUFFER && currentBufferReadLength < currentParsingBuffer.length(); chunkLengthRead++, currentBufferReadLength++) {
            byte b = currentParsingBuffer.getByte(currentBufferReadLength);
            if (';' == b) {
                String s = new String(chunkLengthBytes, 0, chunkLengthRead);
                currentChunkTotalLength = Integer.parseInt(s, HEX);
                currentBufferReadLength++;
                status = STATUS_CHUNK_SIGNATURE;
                return bufferNotFinished();
            }
            if (!((b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || (b >= 'A' && b <= 'Z'))) {
                //非法字符
                throw new MsException(ErrorNo.INCOMPLETE_BODY, "the length includes error chars");
            }
            chunkLengthBytes[chunkLengthRead] = b;
        }
        if (chunkLengthRead >= MAX_CHUNK_SIZE_BUFFER) {
            //块长度过长
            throw new MsException(ErrorNo.INCOMPLETE_BODY, "the length is too long");
        }
        return bufferNotFinished();
    }

    private boolean parseSignature() {
        //开始读取签名
        for (; chunkSignatureRead < PART && currentBufferReadLength < currentParsingBuffer.length(); chunkSignatureRead++, currentBufferReadLength++) {
            byte b = currentParsingBuffer.getByte(currentBufferReadLength);

            if ('\n' == b && '\r' == chunkSignatureBytes[chunkSignatureRead - 1]) {
                String chunkSignature = new String(chunkSignatureBytes, 0, chunkSignatureRead).replaceAll("\\s+", "");
                chunkSignatureArray = chunkSignature.split("=");
                if (2 != chunkSignatureArray.length || 64 != chunkSignatureArray[1].length()) {
                    throw new MsException(ErrorNo.INCOMPLETE_BODY, "the signature chunk is error");
                }
                chunkSignatureRead++;
                currentBufferReadLength++;
                sha256.reset();
                if (currentChunkTotalLength == 0){
                    zeroDataChunkSignature = chunkSignatureArray[1];
                    status = STATUS_CHUNK_TRAILER_HEADER;
                }else {
                    status = STATUS_CHUNK_DATA;
                }
                return bufferNotFinished();
            }
            if (!((b >= 'a' && b <= 'z')
                    || (b >= '0' && b <= '9')
                    || '-' == b
                    || '=' == b
                    || '\r' == b
                    || ' ' == b)) {
                //非法字符
                throw new MsException(ErrorNo.INCOMPLETE_BODY, "the signature includes error chars");

            }
            chunkSignatureBytes[chunkSignatureRead] = b;
        }
        if (chunkSignatureRead >= PART) {
            throw new MsException(ErrorNo.INCOMPLETE_BODY, "the signature chunk is too long");
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

    private boolean parseSuffixR() throws DecoderException {
        checkChunkSignature();
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
            chunkSignatureRead = 0;
            currentBufferReadLength++;
        } else {
            throw new MsException(ErrorNo.INCOMPLETE_BODY, "the char is not n");
        }
        return bufferNotFinished();
    }

    private boolean parseTrailerHeader() {
        //读取签名块头部名称
        for (; trailerHeaderRead < PART && currentBufferReadLength < currentParsingBuffer.length(); trailerHeaderRead++, currentBufferReadLength++) {
            byte b = currentParsingBuffer.getByte(currentBufferReadLength);
            if (':' == b) {
                trailerChunkHead = new String(chunkTrailerHeaderBytes, 0, trailerHeaderRead).trim();
                //检验头
                if (!(trailerChunkHead.equals(request.headers().get(X_AMZ_TRAILER)) && X_AMZ_TRAILER_ALGORITHM.equals(trailerChunkHead))){
                    throw new MsException(ErrorNo.INCOMPLETE_BODY, "trailer chunk header is error");
                }
                trailerHeaderRead = 0;
                currentBufferReadLength++;
                status = STATUS_CHUNK_TRAILER_VALUE;
                return bufferNotFinished();
            }
            if (!((b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || (b >= 'A' && b <= 'Z') || (b == '-'))) {
                //非法字符
                throw new MsException(ErrorNo.INCOMPLETE_BODY, "the trailer header includes error chars");
            }
            chunkTrailerHeaderBytes[trailerHeaderRead] = b;
        }
        if (trailerHeaderRead >= PART) {
            //块长度过长
            throw new MsException(ErrorNo.INCOMPLETE_BODY, "the trailer header is too long");
        }
        return bufferNotFinished();
    }

    private boolean parseTrailerValue() {
        //开始读取校验值
        for (; crcRead < PART && currentBufferReadLength < currentParsingBuffer.length(); crcRead++, currentBufferReadLength++) {
            byte b = currentParsingBuffer.getByte(currentBufferReadLength);
            if ('\n' == b && '\r' == crcCheckbytes[crcRead - 1]) {
                trailerChunkValue = new String(crcCheckbytes, 0, crcRead).replaceAll("\\s+", "");
                //校验值
                String serverCrcValue = BigEndianCRC32.longToBigEndianBase64(crc32.getValue());
                if (!serverCrcValue.equals(trailerChunkValue)) {
                    throw new MsException(ErrorNo.INCOMPLETE_BODY, "CRC32 checksum is not match with body");
                }
                request.response().putHeader(X_AMZ_TRAILER_ALGORITHM,serverCrcValue);
                crcRead = 0;
                currentBufferReadLength++;
                status = STATUS_CHUNK_TRAILER_SIGNATURE;
                return bufferNotFinished();
            }
            if (!((b >= 'a' && b <= 'z')
                    || (b >= '0' && b <= '9')
                    || (b >= 'A' && b <= 'Z')
                    || '-' == b
                    || '=' == b
                    || '\r' == b
                    || '\n' == b
                    || ' ' == b)) {
                //非法字符
                throw new MsException(ErrorNo.INCOMPLETE_BODY, "the trailer value includes error chars");

            }
            crcCheckbytes[crcRead] = b;
        }
        if (crcRead >= PART) {
            throw new MsException(ErrorNo.INCOMPLETE_BODY, "the trailer value is too long");
        }
        return bufferNotFinished();
    }

    private boolean parseTrailerSignature() {
        //开始读取校验值
        for (; trailerSignatureRead < PART && currentBufferReadLength < currentParsingBuffer.length(); trailerSignatureRead++, currentBufferReadLength++) {
            byte b = currentParsingBuffer.getByte(currentBufferReadLength);
            if ('\n' == b && '\r' == trailerSignatureBytes[trailerSignatureRead - 1]) {
                String trailerChunkSignature = new String(trailerSignatureBytes, 0, trailerSignatureRead).replaceAll("\\s+", "");
                String[] trailerChunkSignatureArray = trailerChunkSignature.split(":");
                if (2 != trailerChunkSignatureArray.length || 64 != trailerChunkSignatureArray[1].length()){
                    throw new MsException(ErrorNo.INCOMPLETE_BODY, "the trailer signature is error");
                }
                checkTrailerChunkSignature(trailerChunkSignatureArray[1]);
                //校验值
                trailerSignatureRead = 0;
                currentBufferReadLength++;
                status = STATUS_CHUNK_LENGTH;
                return false;
            }
            if (!((b >= 'a' && b <= 'z')
                    || (b >= '0' && b <= '9')
                    || ':' == b
                    || '-' == b
                    || '=' == b
                    || '\r' == b
                    || '\n' == b
                    || ' ' == b)) {
                //非法字符
                throw new MsException(ErrorNo.INCOMPLETE_BODY, "the trailer signature includes error chars");
            }
            trailerSignatureBytes[trailerSignatureRead] = b;
        }
        if (trailerSignatureRead >= PART) {
            throw new MsException(ErrorNo.INCOMPLETE_BODY, "the trailer signature is too long");
        }
        return false;
    }

    private boolean bufferNotFinished() {
        //判断当前buffer读完
        return currentBufferReadLength < currentParsingBuffer.length();
    }

    private void checkChunkSignature() throws DecoderException {
        String currentChunkSHA256 = Hex.encodeHexString(sha256.digest());
        String stringToSign = stringToSignNoHash + lastChunkSignature + "\n"
                + AuthorizeV4.EMPTY_SHA256 + "\n" + currentChunkSHA256;
        lastChunkSignature = Hex.encodeHexString(AuthorizeV4.hmacSHA256(stringToSign, Hex.decodeHex(signatureKey.toCharArray())));
        if (!lastChunkSignature.equals(chunkSignatureArray[1])) {
            //签名不一致
            logger.info("The chunk signature is wrong. signature in chunk is [" + chunkSignatureArray[1]
                    + "].the calculated is [" + lastChunkSignature + "]\n chunk string to sign is[" + stringToSign + "]");
            throw new MsException(ErrorNo.SIGNATURE_DOES_NOT_MATCH, "chunk signature does not match");
        }
    }

    private void checkTrailerChunkSignature(String trailerChunkSignature) {
        String stringToSign = null;
        try {
            String SignedHeaders = trailerChunkHead + ":" + trailerChunkValue + "\n";
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(SignedHeaders.getBytes(StandardCharsets.UTF_8));
            String currentChunkHash = Hex.encodeHexString(hash);
            stringToSign = stringToSignNoHash.replace("PAYLOAD", "TRAILER") + zeroDataChunkSignature + "\n" + currentChunkHash;
            lastChunkSignature = Hex.encodeHexString(AuthorizeV4.hmacSHA256(stringToSign, Hex.decodeHex(signatureKey.toCharArray())));
        } catch (NoSuchAlgorithmException | DecoderException e) {
            throw new RuntimeException(e);
        }
        if (!lastChunkSignature.equals(trailerChunkSignature)) {
            //签名不一致
            logger.info("The chunk signature is wrong. signature in chunk is [" + trailerChunkSignature
                    + "].the calculated is [" + lastChunkSignature + "]\n chunk string to sign is\n[" + stringToSign + "]");
            throw new MsException(ErrorNo.SIGNATURE_DOES_NOT_MATCH, "trailer chunk signature does not match");
        }
    }
}
