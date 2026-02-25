package com.macrosan.message.xmlmsg;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * @author zhaoyang
 * @date 2024/01/12
 **/

@Data
@Accessors(chain = true)
@XmlRootElement(name = "Mark")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Mark", propOrder = {
        "mark",
})
public class ErrorMark {
    @XmlElement(name = "Mark", required = true)
    private String mark;
}
