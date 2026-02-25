package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.UserInfos;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * UserInfosResponse
 * 保存用户列表响应信息的容器
 *
 * @author shilinyong
 * @date 2019/07/19
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "UserInfosResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "UserInfosResponse", propOrder = {
        "userInfos"
})
public class UserInfosResponse {

    @XmlElement(name = "UserInfos", required = true)
    private UserInfos userInfos;
}
