package com.macrosan.message.xmlmsg;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * @author zhaoyang
 * @date 2023/12/08
 **/

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ComponentObjectErrorRecord")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ComponentObjectErrorRecord", propOrder = {
        "object",
        "bucket",
        "versionId",
        "errorMessage",
        "errorTime",
        "processType",
        "errorMark"
})
public class ComponentObjectErrorResponse {


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

    @XmlElement(name = "processType")
    private String processType;

    @XmlElement(name = "errorMark")
    private String errorMark;

}
