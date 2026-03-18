package com.macrosan.message.jsonmsg;

import com.dslplatform.json.JsonAttribute;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.File;

import static com.macrosan.constants.SysConstants.ROCKS_FILE_META_PREFIX;

/**
 * @author gaozhiyuan
 */
@Data
@Accessors(chain = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class FileMeta {
    @JsonAttribute
    @JsonProperty("1")
    @JsonAlias({"lun"})
    String lun;
    @JsonAttribute
    @JsonProperty("2")
    @JsonAlias({"fileName"})
    String fileName;

    /**
     * 旧版使用md5算法，新版使用crc32算法；可根据etag长度区分是由md5还是crc32算法生成的
     * md5算法生成的etag长度为32位
     * crc32算法生成的etag长度为8位
     */
    @JsonAttribute
    @JsonProperty("3")
    @JsonAlias({"etag"})
    String etag;
    @JsonAttribute
    @JsonProperty("4")
    @JsonAlias({"size"})
    long size;
    boolean smallFile;
    @JsonAttribute
    @JsonProperty("5")
    @JsonAlias({"offset"})
    long[] offset;
    @JsonAttribute
    @JsonProperty("6")
    @JsonAlias({"len"})
    long[] len;
    @JsonAttribute
    @JsonProperty("7")
    @JsonAlias({"metaKey"})
    String metaKey;

    String compression;
    long[] compressBeforeLen;
    long[] compressAfterLen;
    long[] compressState;

    String crypto;
    String secretKey;
    long[] cryptoBeforeLen;
    long[] cryptoAfterLen;
    String cryptoVersion;

    @JsonAttribute
    @JsonProperty("8")
    @JsonAlias({"fileOffset"})
    long fileOffset;
    String flushStamp;

    //增加最后访问时间
    String lastAccessStamp;

    public void setKey(String partKey) {

    }

    @JsonIgnore
    public String getKey() {
        return getKey(fileName);
    }


    public static String getKey(String fileName) {
        return ROCKS_FILE_META_PREFIX + fileName.split(File.separator)[1];
    }
}
