package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/8/3 19:27
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "AttachUserPolicyResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AttachUserPolicyResponse",propOrder = {
        "responseMetadata"
})
public class AttachUserPolicyResponse {
    @XmlElement(name = "ResponseMetadata",required = true)
    ResponseMetadata responseMetadata;
}

