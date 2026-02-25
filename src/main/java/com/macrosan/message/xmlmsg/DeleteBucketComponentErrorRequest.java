package com.macrosan.message.xmlmsg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * @author zhaoyang
 * @date 2024/01/12
 **/

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "DeleteBucketComponentErrorRequest")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(namespace = "DeleteBucketComponentErrorRequest", propOrder = {
        "clear",
        "errorMarks"
})
public class DeleteBucketComponentErrorRequest {
    @XmlElement(name = "Clear", required = false)
    private String clear;

    @XmlElementWrapper(name = "ErrorRecords")
    @XmlElement(name = "ErrorRecord", required = true)
    private List<ErrorMark> errorMarks;
}
