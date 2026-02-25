package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * AuthInfos
 * 保存权限信息列表的容器
 *
 * @author shilinyong
 * @date 2019/08/13
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "AuthInfos")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AuthInfos", propOrder = {
        "authInfo",
})
public class AuthInfos {

    @XmlElement(name = "AuthInfo", required = true)
    private List<AuthInfo> authInfo;
}
