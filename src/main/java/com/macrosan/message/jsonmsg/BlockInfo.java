package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.macrosan.database.rocksdb.MossMergeOperator.SPACE_LEN;
import static com.macrosan.database.rocksdb.MossMergeOperator.SPACE_SIZE;
import static com.macrosan.fs.BlockDevice.BLOCK_SIZE;
import static com.macrosan.fs.BlockDevice.ROCKS_FILE_SYSTEM_PREFIX_OFFSET;

/**
 * @author gaozhiyuan
 */
@Data
@CompiledJson
@Accessors(chain = true)
public class BlockInfo {
    @JsonAttribute
    public String[] fileName;

    @JsonAttribute
    public long offset;

    @JsonAttribute
    public long[] len;

    @JsonAttribute
    public long total;

    public void setKey(String partKey) {

    }

    public String getKey() {
        return getKey(offset);
    }

    /**
     * PB
     */
    private static final int OFFSET_LEN = String.valueOf(1024L * 1024 * 1024 * 1024 * 1024).length();

    public static String getKey(long offset) {
        String offsetStr = String.valueOf(offset);
        if (offsetStr.length() < OFFSET_LEN) {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < OFFSET_LEN - offsetStr.length(); i++) {
                builder.append('0');
            }

            builder.append(offsetStr);
            return ROCKS_FILE_SYSTEM_PREFIX_OFFSET + builder.toString();
        } else {
            return ROCKS_FILE_SYSTEM_PREFIX_OFFSET + offset;
        }
    }

    public static String getFamilySpaceKey(long index) {
        StringBuilder sb = new StringBuilder();
        sb.append(".1.");
        String s = String.valueOf(index);
        for (int i = 0; i < 10 - s.length(); i++) {
            sb.append("0");
        }
        return sb.append(s).toString();
    }

    public static byte[] getBitArray(byte b) {
        byte[] array = new byte[8];
        for (int i = 7; i >= 0; i--) {
            array[i] = (byte) (b & 1);
            b = (byte) (b >> 1);
        }
        return array;
    }

    public static List<byte[]> getUpdateValue(long offset, long size, String type) {
        ArrayList<byte[]> list = new ArrayList<>();
        long keyStart = offset / SPACE_SIZE;
        long keyOffset = (offset + size) / SPACE_SIZE;
        if ((offset + size) % SPACE_SIZE == 0) {
            keyOffset -= 1;
        }
        int markStart = (int) (offset - keyStart * SPACE_SIZE) / BLOCK_SIZE / 8;
        int markBitStartIndex = (int) (offset - keyStart * SPACE_SIZE) / BLOCK_SIZE % 8;

        long keyEnd = keyOffset;
        int markBitEndIndex = (int) ((size + offset - keyOffset * SPACE_SIZE) / BLOCK_SIZE % 8);
        int markEnd = (int) ((size + offset - keyOffset * SPACE_SIZE) / BLOCK_SIZE / 8);
        if (size % BLOCK_SIZE > 0) {
            markBitEndIndex += 1;
        }
        if (markBitEndIndex == 0) {
            if (markEnd == 0) {
                keyEnd -= 1;
            }
            markBitEndIndex = 8;
            markEnd -= 1;
        }
        if (keyStart == keyEnd) {
            list.add(getSimpleValue(markStart, markEnd, markBitStartIndex, markBitEndIndex, type));
        } else {
            for (long start = keyStart; start <= keyEnd; start++) {
                if (start == keyStart) {
                    list.add(getSimpleValue(markStart, SPACE_LEN - 1, markBitStartIndex, 8, type));
                } else if (start != keyEnd) {
                    list.add(getSimpleValue(0, SPACE_LEN - 1, 0, 8, type));
                } else {
                    list.add(getSimpleValue(0, markEnd, 0, markBitEndIndex, type));
                }
            }
        }
        return list;
    }

    private static byte[] getSimpleValue(int markStart, int markEnd, int markBitStartIndex, int markBitEndIndex, String type) {
        byte[] value = new byte[SPACE_LEN * 2];
        if ("upload".equals(type)) {
            Arrays.fill(value, SPACE_LEN, SPACE_LEN * 2, (byte) -1);
            if (markEnd == markStart) {
                for (int i = markBitEndIndex - 1; i >= markBitStartIndex; i--) {
                    value[markStart] |= (byte) (1 << 7 - i);
                }
            } else {
                for (int i = markStart; i <= markEnd; i++) {
                    if (i != markStart && i != markEnd) {
                        value[i] = -1;
                    } else {
                        if (i == markStart) {
                            for (int j = 7; j >= markBitStartIndex; j--) {
                                value[i] |= (byte) (1 << 7 - j);
                            }
                        } else {
                            for (int j = 0; j < markBitEndIndex; j++) {
                                value[i] |= (byte) (1 << 7 - j);
                            }
                        }
                    }
                }
            }
//            value[0] |= -128;
        } else if ("delete".equals(type)) {
            Arrays.fill(value, SPACE_LEN, SPACE_LEN * 2, (byte) -1);
            if (markEnd == markStart) {
                for (int i = markBitEndIndex - 1; i >= markBitStartIndex; i--) {
                    value[markStart + SPACE_LEN] ^= (byte) (1 << 7 - i);
                }
            } else {
                for (int i = markStart; i <= markEnd; i++) {
                    if (i != markStart && i != markEnd) {
                        value[i + SPACE_LEN] = 0;
                    } else {
                        if (i == markStart) {
                            for (int j = 7; j >= markBitStartIndex; j--) {
                                value[i + SPACE_LEN] ^= (byte) (1 << 7 - j);
                            }
                        } else {
                            for (int j = 0; j < markBitEndIndex; j++) {
                                value[i + SPACE_LEN] ^= (byte) (1 << 7 - j);
                            }
                        }
                    }
                }
            }
        }
        return value;
    }


}
