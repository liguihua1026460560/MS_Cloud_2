package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/7/31 19:27
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "UpdateGroupResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "UpdateGroupResponse", propOrder = {
        "responseMetadata"
})
public class UpdateGroupResponse {
    @XmlElement(name = "ResponseMetadata", required = true)
    ResponseMetadata responseMetadata;
}
