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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Copyright 2020 MacroSAN, Co.Ltd. All rights reserved.
 * 类名称  ：V4DataStreamHandler
 * 描述    ：
 * 作者    ：yiguangzheng
 * 创建时间：2020/2/5
 * *******************************************************
 * 修改时间        修改人             修改原因
 * *******************************************************
 * 2020/2/5         yiguangzheng      初始版本
 */
public class V4DataStreamHandler implements Handler<Buffer> {
    private static final Logger logger = LogManager.getLogger(V4DataStreamHandler.class);
    private static final int HEX = 16;
    private static final int PART = 1024;
    //块长度最大10字节  即块的最大长度 2^40 - 1
    private static final int MAX_CHUNK_SIZE_BUFFER = 10;

    private static final int STATUS_CHUNK_LENGTH = 0;
    private static final int STATUS_CHUNK_SIGNATURE = 1;
    private static final int STATUS_CHUNK_DATA = 2;
    private static final int STATUS_CHUNK_SUFFIX_R = 3;
    private static final int STATUS_CHUNK_SUFFIX_N = 4;

    private Consumer<byte[]> bytesConsumer;
    private Consumer<Throwable> errorConsumer;
    private MsHttpRequest request;
    @Getter
    private MessageDigest sha256;
    private String lastChunkSignature;
    private String stringToSignNoHash;
    private String signatureKey;
    /**
     * handler 状态
     * 0：读数据块长度
     * 1：读数据块签名
     * 2：读数据
     * 3：读数据块结束符\r
     * 4：读数据块结束符\n
     */
    private int status = STATUS_CHUNK_LENGTH;
    private Buffer currentParsingBuffer;
    private int currentBufferReadLength = 0;
    private int currentChunkTotalLength = -1;
    private int currentChunkReadLength = 0;
    private byte[] chunkLengthBytes = new byte[MAX_CHUNK_SIZE_BUFFER];
    private int chunkLengthRead = 0;
    private byte[] chunkSignatureBytes = new byte[PART];
    private int chunkSignatureRead = 0;
    private String[] chunkSignatureArray;

    public V4DataStreamHandler(MsHttpRequest request, Consumer<byte[]> bytesConsumer, Consumer<Throwable> errorConsumer) {
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
            //多块
            if (AuthorizeV4.CONTENT_SHA256_MULTIPLE_CHUNKS_PAYLOAD.equals(request.getHeader(AuthorizeV4.X_AMZ_CONTENT_SHA_256))) {
                parse(event);
            } else {
                //单块
                byte[] bytes = event.getBytes();
                // UNSIGNED-PAYLOAD 不需要计算 SHA256
                if (!AuthorizeV4.CONTENT_SHA256_UNSIGNED_PAYLOAD.equals(request.getHeader(AuthorizeV4.X_AMZ_CONTENT_SHA_256))){
                    sha256.update(bytes);
                }
                Optional.ofNullable(bytesConsumer).ifPresent(consumer -> consumer.accept(bytes));
                bytesConsumer.accept(new byte[0]);
            }
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
                throw new MsException(ErrorNo.INCOMPLETE_BODY, "chunk data error");
            }
            chunkLengthBytes[chunkLengthRead] = b;
        }
        if (chunkLengthRead >= MAX_CHUNK_SIZE_BUFFER) {
            //块长度过长
            logger.info("The chunk is too long.");
            throw new MsException(ErrorNo.INCOMPLETE_BODY, "chunk data error");
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
                    throw new MsException(ErrorNo.INCOMPLETE_BODY, "chunk data error");
                }

                chunkSignatureRead++;
                currentBufferReadLength++;
                status = STATUS_CHUNK_DATA;
                sha256.reset();
                return bufferNotFinished();
            }
            if (!((b >= 'a' && b <= 'z')
                    || (b >= '0' && b <= '9')
                    || '-' == b
                    || '=' == b
                    || '\r' == b
                    || ' ' == b)) {
                //非法字符
                throw new MsException(ErrorNo.INCOMPLETE_BODY, "chunk data error");

            }
            chunkSignatureBytes[chunkSignatureRead] = b;
        }
        if (chunkSignatureRead >= PART) {
            logger.info("The chunk signature is too long.");
            throw new MsException(ErrorNo.INCOMPLETE_BODY, "chunk data error");
        }

        return bufferNotFinished();

    }

    private boolean parseData() {
        int readLen = Math.min(currentParsingBuffer.length() - currentBufferReadLength, currentChunkTotalLength - currentChunkReadLength);
        if (readLen > 0) {
            byte[] bytes = currentParsingBuffer.getBytes(currentBufferReadLength, currentBufferReadLength + readLen);
            sha256.update(bytes);
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
            throw new MsException(ErrorNo.INCOMPLETE_BODY, "chunk data error");
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
            throw new MsException(ErrorNo.INCOMPLETE_BODY, "chunk data error");
        }
        return bufferNotFinished();
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
}


