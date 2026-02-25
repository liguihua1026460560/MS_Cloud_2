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
 * @className ObjectLockConfiguration
 * @date 2021/9/6 14:17
 */
@Data
@XmlRootElement(name = "ObjectLockConfiguration")
@XmlAccessorType(XmlAccessType.FIELD)
public class ObjectLockConfiguration {

    @XmlElement(name = "ObjectLockEnabled",required = false)
    private String objectLockStatus;

    @XmlElement(name = "Rule", required = true)
    private Rule rule;

}