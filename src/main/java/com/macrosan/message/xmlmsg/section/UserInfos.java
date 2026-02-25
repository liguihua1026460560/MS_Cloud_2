package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

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
@XmlRootElement(name = "UserInfos")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "UserInfos", propOrder = {
        "userInfo",
})
public class UserInfos {

    @XmlElement(name = "UserInfo", required = true)
    private List<UserInfo> userInfo;
}
