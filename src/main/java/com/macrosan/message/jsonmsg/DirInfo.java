package com.macrosan.message.jsonmsg;


import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@CompiledJson
@Accessors(chain = true)
public class DirInfo {

    @JsonAttribute
    public String dirName;
    @JsonAttribute
    public String usedObjects = "0";
    @JsonAttribute
    public String usedCap = "0";
    @JsonAttribute
    public String flag;

    public static final DirInfo ERROR_DIR_INFO = new DirInfo().setFlag("error");
    public static final DirInfo NOT_FOUND_DIR_INFO = new DirInfo().setFlag("not found");

    public static boolean isErrorInfo(DirInfo dirInfo) {
        return "error".equals(dirInfo.getFlag());
    }
}
