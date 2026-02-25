package com.macrosan.message.xmlmsg.worm;

import lombok.Data;

import javax.xml.bind.annotation.*;

/**
 * <p></p>
 *
 * @author Administrator
 * @version 1.0
 * @className Retention
 * @date 2021/9/17 10:10
 */
@Data
@XmlRootElement(name = "Retention")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Retention", propOrder = {
        "mode",
        "retainUntilDate"
})
public class Retention {

    @XmlElement(name = "Mode", required = false)
    private String mode;

    @XmlElement(name = "RetainUntilDate", required = true)
    private String retainUntilDate;

}
