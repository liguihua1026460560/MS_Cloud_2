package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/8/1 14:41
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "Member")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Member",namespace = "Policy",propOrder = {
        "arn",
        "attachmentCount",
        "createDate",
        "defaultVersionId",
        "isAttachable",
        "path",
        "permissionsBoundaryUsageCount",
        "policyId",
        "policyName",
        "updateDate",
        "description"
})
public class PolicyMember {
    @XmlElement(name = "Arn",required = true)
    String arn;
    @XmlElement(name = "AttachmentCount",required = true)
    String attachmentCount;
    @XmlElement(name = "CreateDate",required = true)
    String createDate;
    @XmlElement(name = "DefaultVersionId",required = true)
    String defaultVersionId;
    @XmlElement(name = "IsAttachable",required = true)
    boolean isAttachable;
    @XmlElement(name = "Path",required = true)
    String path;
    @XmlElement(name = "PermissionsBoundaryUsageCount",required = true)
    String permissionsBoundaryUsageCount;
    @XmlElement(name = "PolicyId",required = true)
    String policyId;
    @XmlElement(name = "PolicyName",required = true)
    String policyName;
    @XmlElement(name = "UpdateDate",required = true)
    String updateDate;
    @XmlElement(name = "Description",required = false)
    String description;
}
