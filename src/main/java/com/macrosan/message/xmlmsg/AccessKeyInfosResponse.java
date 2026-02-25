package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.AccessKeyInfos;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * AccessKeyInfosResponse
 * 保存用户ak列表响应信息的容器
 *
 * @author shilinyong
 * @date 2019/08/01
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "AccessKeyInfosResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AccessKeyInfosResponse", propOrder = {
        "accessKeyInfos"
})
public class AccessKeyInfosResponse {

    @XmlElement(name = "AccessKeyInfos", required = true)
    private AccessKeyInfos accessKeyInfos;
}
