package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author shilinyong
 * @Copyright MacroSAN Technologies Co., Ltd. All rights reserved.
 * @date 2019年07月19日 下午2:59:46
 */
@Data
@CompiledJson
@Accessors(chain = true)
public class UserInfoVO {
    @JsonAttribute
    public int id;
    @JsonAttribute
    public String userId;
    @JsonAttribute
    public String accountId;
    @JsonAttribute
    public String userName;
    @JsonAttribute
    public String nickname;
    @JsonAttribute
    public String passwd;
    @JsonAttribute
    public String mobile;
    @JsonAttribute
    public String email;
    @JsonAttribute
    public String createTime;
    @JsonAttribute
    public String mrn;
    @JsonAttribute
    public String remark;
    @JsonAttribute
    public int isProgrammeAccess;
    @JsonAttribute
    public int isConsoleAccess;
}
