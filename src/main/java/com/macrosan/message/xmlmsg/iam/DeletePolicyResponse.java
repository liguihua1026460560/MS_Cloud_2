package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/8/1 14:05
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "DeletePolicyResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DeletePolicyResponse", propOrder = {
        "responseMetadata"
})
public class DeletePolicyResponse {
    @XmlElement(name = "ResponseMetadata",required = true)
    ResponseMetadata responseMetadata;
}
