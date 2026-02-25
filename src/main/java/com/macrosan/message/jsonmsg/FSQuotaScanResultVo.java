package com.macrosan.message.jsonmsg;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class FSQuotaScanResultVo {
    public long fileCount;
    public long fileTotalSize;
    public long dirCount;
    public String marker;
}
