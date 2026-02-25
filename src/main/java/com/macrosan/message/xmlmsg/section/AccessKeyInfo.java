package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * AccessKeyInfo
 * 保存用户AK信息的容器
 *
 * @author shilinyong
 * @date 2019/08/01
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "AccessKeyInfo")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AccessKeyInfo", propOrder = {
        "accessKeyId",
        "accessKeySecret",
        "createTime",
})
public class AccessKeyInfo {

    @XmlElement(name = "AccessKeyId", required = true)
    private String accessKeyId;
    @XmlElement(name = "AccessKeySecret", required = true)
    private String accessKeySecret;
    @XmlElement(name = "CreateTime", required = true)
    private String createTime;
}
