package com.macrosan.utils.msutils.md5;

import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.utils.cache.FastMd5DigestPool;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static com.macrosan.constants.SysConstants.SYNC_COMPRESS;

public class PartMd5Digest extends Digest {

    Digest digest;
    Digest part;
    List<byte[]> partMD5 = new LinkedList<>();
    long partSize;
    long index = 0;

    public PartMd5Digest(long partSize, MsHttpRequest request) {
        this.partSize = partSize;
        if (FastMd5Digest.isAvailable() && StringUtils.isBlank(request.getHeader(SYNC_COMPRESS))) {
            try {
                digest = FastMd5DigestPool.getInstance().borrow();
            } catch (Exception e) {
                digest = new Md5Digest();
            }
            try {
                part = FastMd5DigestPool.getInstance().borrow();
            } catch (Exception e) {
                part = new Md5Digest();
            }
        } else {
            digest = new Md5Digest();
            part = new Md5Digest();
        }

    }

    @Override
    public void update(byte[] bytes) {
        digest.update(bytes);
        index += bytes.length;

        if (index >= partSize) {
            int last = (int) (index - partSize);
            if (last == 0) {
                part.update(bytes);
            } else {
                part.update(Arrays.copyOfRange(bytes, 0, bytes.length - last));
            }
            partMD5.add(part.digest());
            index = last;
            if (last != 0) {
                part.update(Arrays.copyOfRange(bytes, bytes.length - last, bytes.length));
            }
        } else {
            part.update(bytes);
        }
    }

    @Override
    public void update(ByteBuffer buffer) {
        int remaining = buffer.remaining();
        index += remaining;
        // 保存原始位置和限制
        int originalPosition = buffer.position();
        int originalLimit = buffer.limit();
        // 更新主摘要
        digest.update(buffer);
        // 重置buffer位置用于part处理
        buffer.position(originalPosition);
        if (index >= partSize) {
            int last = (int) (index - partSize);
            if (last == 0) {
                part.update(buffer);
            } else {
                // 分割buffer：第一部分用于完成当前part
                int firstPartLength = remaining - last;
                buffer.limit(buffer.position() + firstPartLength);
                part.update(buffer);

                // 计算当前part的摘要
                partMD5.add(part.digest());
                index = last;

                if (last > 0) {
                    // 剩余部分作为下一个part的开始
                    buffer.limit(originalLimit);
                    part.update(buffer);
                }
            }
        } else {
            // 如果index小于partSize，直接更新part digest
            part.update(buffer);
        }
    }

    @Override
    public void reset() {
        digest.reset();
        part.reset();
    }

    @Override
    public byte[] digest() {
        if (index > 0) {
            partMD5.add(part.digest());
        }
        return digest.digest();
    }

    public String[] getPartMd5() {
        String[] res = new String[partMD5.size()];
        int i = 0;
        for (byte[] bytes : partMD5) {
            res[i++] = Hex.encodeHexString(bytes);
        }
        return res;
    }

    public String getPartMd5(int partN) {
        String res = Hex.encodeHexString(partMD5.get(partN));
        return res;
    }

    public void returnToPool() {
        if (digest instanceof FastMd5Digest) {
            FastMd5DigestPool.getInstance().returnDigest(digest);
        }
        if (part instanceof FastMd5Digest) {
            FastMd5DigestPool.getInstance().returnDigest(part);
        }
    }

    public void invalidateToPool() {
        if (digest instanceof FastMd5Digest) {
            FastMd5DigestPool.getInstance().invalidateDigest(digest);
        }
        if (part instanceof FastMd5Digest) {
            FastMd5DigestPool.getInstance().invalidateDigest(part);
        }
    }
}
