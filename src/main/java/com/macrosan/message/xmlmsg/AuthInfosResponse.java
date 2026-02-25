package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.AuthInfos;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * AuthInfosResponse
 * 保存权限列表响应信息的容器
 *
 * @author shilinyong
 * @date 2019/08/13
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "AuthInfosResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AuthInfosResponse", propOrder = {
        "authInfos"
})
public class AuthInfosResponse {

    @XmlElement(name = "AuthInfos", required = true)
    private AuthInfos authInfos;
}
