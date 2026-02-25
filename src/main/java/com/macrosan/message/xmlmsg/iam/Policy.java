package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/8/1 11:55
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "Policy")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Policy", propOrder = {
        "policyName",
        "description",
        "defaultVersionId",
        "policyId",
        "path",
        "arn",
        "attachmentCount",
        "createDate",
        "updateDate"
})
public class Policy {
    @XmlElement(name = "PolicyName", required = true)
    String policyName;
    @XmlElement(name = "Description", required = true)
    String description;
    @XmlElement(name = "DefaultVersionId", required = true)
    String defaultVersionId;
    @XmlElement(name = "PolicyId", required = true)
    String policyId;
    @XmlElement(name = "Path", required = true)
    String path;
    @XmlElement(name = "Arn", required = true)
    String arn;
    @XmlElement(name = "AttachmentCount", required = true)
    int attachmentCount;
    @XmlElement(name = "CreateDate", required = true)
    String createDate;
    @XmlElement(name = "UpdateDate", required = true)
    String updateDate;
}
