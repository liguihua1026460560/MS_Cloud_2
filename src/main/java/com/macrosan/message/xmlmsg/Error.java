package com.macrosan.message.xmlmsg;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Error
 * error.xml的实体类
 *
 * @author liyixin
 * @date 2018/11/12
 */
@Data
@Accessors(chain = true)
@XmlRootElement(name = "Error")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Error", propOrder = {
        "code",
        "message",
        "resource",
        "hostId",
        "requestId",
        "key",
        "versionId"
})
public class Error {

    @XmlElement(name = "Key", required = false)
    private String key;

    @XmlElement(name = "VersionId", required = false)
    private String versionId;

    @XmlElement(name = "Code", required = true)
    private String code;

    @XmlElement(name = "Message", required = true)
    private String message;

    @XmlElement(name = "Resource", required = true)
    private String resource;

    @XmlElement(name = "HostId", required = true)
    private String hostId;

    @XmlElement(name = "RequestId", required = true)
    private String requestId;

}
