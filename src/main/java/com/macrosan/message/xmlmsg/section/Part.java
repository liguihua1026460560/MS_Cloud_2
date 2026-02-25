package com.macrosan.message.xmlmsg.section;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Part
 * 保存 Part 信息的容器
 *
 * @author liyixin
 * @date 2018/11/13
 */
@Data
@Accessors(chain = true)
@XmlRootElement(name = "Part")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Part", propOrder = {
        "partNumber",
        "lastModified",
        "etag",
        "size"
})

public class Part {
    @XmlElement(name = "PartNumber", required = true)
    private Integer partNumber;

    @XmlElement(name = "LastModified", required = true)
    private String lastModified;

    @XmlElement(name = "ETag", required = true)
    private String etag;

    @XmlElement(name = "Size", required = true)
    private Long size;
    @JsonIgnore
    @XmlTransient
    private String fileName;
}
