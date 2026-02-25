package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.AccessKeyInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * AccessKeyInfoResponse
 * 保存用户AK响应信息的容器
 *
 * @author shilinyong
 * @date 2019/08/01
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "AccessKeyInfoResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AccessKeyInfoResponse", propOrder = {
        "accessKeyInfo"
})
public class AccessKeyInfoResponse {

    @XmlElement(name = "AccessKeyInfo", required = true)
    private AccessKeyInfo accessKeyInfo;
}
