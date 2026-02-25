package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/8/4 9:53
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "DetachGroupPolicyResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DetachGroupPolicyResponse",propOrder = {
        "responseMetadata"
})
public class DetachGroupPolicyResponse {
    @XmlElement(name = "ResponseMetadata",required = true)
    ResponseMetadata responseMetadata;
}
