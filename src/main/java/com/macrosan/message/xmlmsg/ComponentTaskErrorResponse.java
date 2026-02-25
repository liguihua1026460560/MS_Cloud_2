package com.macrosan.message.xmlmsg;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * @author zhaoyang
 * @date 2023/11/23
 **/

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ComponentErrorRecord")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ComponentErrorRecord", propOrder = {
        "object",
        "bucket",
        "versionId",
        "errorMessage",
        "errorTime",
        "errorMark"
})
public class ComponentTaskErrorResponse {

    @XmlElement(name = "object")
    private String object;

    @XmlElement(name = "bucket")
    private String bucket;

    @XmlElement(name = "versionId")
    private String versionId;

    @XmlElement(name = "errorMessage")
    private String errorMessage;

    @XmlElement(name = "errorTime")
    private Long errorTime;

    @XmlElement(name = "errorMark")
    private String errorMark;

}
