package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/7/31 19:23
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "DeleteGroupResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DeleteGroupResponse", propOrder = {
        "responseMetadata"
})
public class DeleteGroupResponse {
    @XmlElement(name = "ResponseMetadata",required = true)
    ResponseMetadata responseMetadata;
}
