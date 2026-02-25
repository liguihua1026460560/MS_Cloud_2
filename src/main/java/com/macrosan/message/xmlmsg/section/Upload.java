package com.macrosan.message.xmlmsg.section;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Upload
 * 保存 Multipart Uploads 请求结果的容器
 *
 * @author liyixin
 * @date 2018/11/13
 */
@Data
@Accessors(chain = true)
@XmlRootElement(name = "Upload")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Upload", propOrder = {
        "key",
        "uploadId",
        "initiator",
        "owner",
        "initiated"
})
public class Upload {

    @XmlElement(name = "Key", required = true)
    private String key;

    @XmlElement(name = "UploadId", required = true)
    private String uploadId;

    @XmlElement(name = "Initiator", required = true)
    private Owner initiator;

    @XmlElement(name = "Owner", required = true)
    private Owner owner;

    @XmlElement(name = "Initiated", required = true)
    private String initiated;
}
