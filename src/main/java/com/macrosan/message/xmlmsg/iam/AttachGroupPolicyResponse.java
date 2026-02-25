package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/8/4 9:50
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "AttachGroupPolicyResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AttachGroupPolicyResponse",propOrder = {
        "responseMetadata"
})
public class AttachGroupPolicyResponse {
    @XmlElement(name = "ResponseMetadata",required = true)
    ResponseMetadata responseMetadata;
}
