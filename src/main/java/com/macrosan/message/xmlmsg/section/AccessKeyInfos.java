package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * AccesskeyInfo
 * 保存用户ak信息的容器
 *
 * @author shilinyong
 * @date 2019/07/19
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "AccessKeyInfos")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AccessKeyInfos", propOrder = {
        "accessKeyInfo",
})
public class AccessKeyInfos {

    @XmlElement(name = "AccessKeyInfo", required = true)
    private List<AccessKeyInfo> accessKeyInfo;
}
