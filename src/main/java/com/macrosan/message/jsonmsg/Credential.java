package com.macrosan.message.jsonmsg;/**
 * @author niechengxing
 * @create 2024-09-12 17:17
 */

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.File;
import java.util.List;

import static com.macrosan.constants.SysConstants.ROCKS_STS_TOKEN_KEY;

/**
 *@program: MS_Cloud
 *@description: STS提供的临时身份凭证
 *@author: niechengxing
 *@create: 2024-09-12 17:17
 */
@Data
@CompiledJson
@Accessors(chain = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class Credential {
    @JsonAttribute
    public String assumeId;

    /**
     * 所扮演的角色所属的账户ID
     */
    @JsonAttribute
    public String accountId;

    /**
     * 所扮演的角色ID
     */
    @JsonAttribute
    public List<String> groupIds;

    @JsonAttribute
    public List<String> policyIds;

    @JsonAttribute
    public InlinePolicy inlinePolicy;

    @JsonAttribute
    public String accessKey;

    @JsonAttribute
    public String secretKey;

    /**
     * 临时会话（凭证）名称
     */
    @JsonAttribute
    public String useName;

    /**
     * 凭证的有限时间
     */
    @JsonAttribute
    public long durationSeconds;

    @JsonAttribute
    public long deadline;

    public static final Credential NOT_FOUND_STS_TOKEN = new Credential().setAssumeId("not found");
    public static final Credential ERROR_STS_TOKEN= new Credential().setAssumeId("error");

    public static String getCredentialKey(String vnode, String accountId, String assumeId, String accessKey){
        return ROCKS_STS_TOKEN_KEY + vnode + File.separator +  accessKey + File.separator + accountId + File.separator + assumeId;
    }

    public static String getCredentialKey(String vnode, String accessKey){
        return ROCKS_STS_TOKEN_KEY + vnode + File.separator +  accessKey;
    }
    public String getCredentialKey(String vnode) {
        return getCredentialKey(vnode, accessKey);
    }

}

