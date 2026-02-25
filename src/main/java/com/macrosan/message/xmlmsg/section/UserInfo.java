package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * UserInfo
 * 保存用户信息的容器
 *
 * @author shilinyong
 * @date 2019/07/19
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "UserInfo")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "UserInfo", propOrder = {
        "userId",
        "userName",
        "userNickName",
        "remark",
        "createTime",
        "userMRN",
        "accessKey",
        "secretKey"
})
public class UserInfo {

    @XmlElement(name = "UserId", required = true)
    private String userId;
    @XmlElement(name = "UserName", required = true)
    private String userName;
    @XmlElement(name = "UserNickName", required = true)
    private String userNickName;
    @XmlElement(name = "Remark", required = true)
    private String remark;
    @XmlElement(name = "CreateTime", required = true)
    private String createTime;
    @XmlElement(name = "UserMRN", required = true)
    private String userMRN;
    @XmlElement(name = "AccessKey", required = true)
    private String accessKey;
    @XmlElement(name = "SecretKey", required = true)
    private String secretKey;
}
