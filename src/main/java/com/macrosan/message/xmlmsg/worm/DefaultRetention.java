package com.macrosan.message.xmlmsg.worm;

import lombok.Data;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * <p></p>
 *
 * @author Administrator
 * @version 1.0
 * @className DefaultRetention
 * @date 2021/9/6 14:28
 */
@Data
@XmlRootElement(name = "DefaultRetention")
@XmlAccessorType(XmlAccessType.FIELD)
public class DefaultRetention {

    @XmlElement(name = "Days", required = false)
    private Integer days;

    @XmlElement(name = "Mode", required = false)
    private String mode;

    @XmlElement(name = "Years", required = false)
    private Integer years;

}
