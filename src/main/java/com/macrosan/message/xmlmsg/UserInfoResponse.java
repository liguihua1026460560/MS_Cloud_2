package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.UserInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * UserInfoResponse
 * 保存用户响应信息的容器
 *
 * @author shilinyong
 * @date 2019/07/19
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "UserInfoResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "UserInfoResponse", propOrder = {
        "userInfo"
})
public class UserInfoResponse {

    @XmlElement(name = "UserInfo", required = true)
    private UserInfo userInfo;
}
