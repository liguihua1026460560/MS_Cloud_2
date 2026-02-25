package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/8/3 17:43
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "RemoveUsersFromGroup")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "RemoveUsersFromGroup",propOrder = {
        "responseMetadata"
})
public class RemoveUsersFromGroupResponse {
    @XmlElement(name = "ResponseMetadata",required = true)
    ResponseMetadata responseMetadata;
}
