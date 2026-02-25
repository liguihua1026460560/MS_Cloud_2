package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "CreateAccessKeyResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CreateAccessKeyResponse", propOrder = {
        "createAccessKeyResult",
        "responseMetadata"
})
public class CreateAccessKeyResponse {
    @XmlElement(name = "CreateAccessKeyResult", required = true)
    CreateAccessKeyResult createAccessKeyResult;
    @XmlElement(name = "ResponseMetadata", required = true)
    ResponseMetadata responseMetadata;

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "CreateAccessKeyResult")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "CreateAccessKeyResult", propOrder = {
            "accessKey"
    })
    public static class CreateAccessKeyResult {
        @XmlElement(name = "AccessKey", required = true)
        AccessKey accessKey;
    }

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "AccessKey")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "AccessKey", propOrder = {
            "userName",
            "accessKeyId",
            "status",
            "secretAccessKey",
            "createDate"
    })
    public static class AccessKey {
        @XmlElement(name = "UserName", required = true)
        String userName;
        @XmlElement(name = "AccessKeyId", required = true)
        String accessKeyId;
        @XmlElement(name = "Status", required = true)
        String status;
        @XmlElement(name = "SecretAccessKey", required = true)
        String secretAccessKey;
        @XmlElement(name = "CreateDate", required = true)
        String createDate;
    }
}
