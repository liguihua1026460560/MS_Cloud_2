package com.macrosan.message.xmlmsg.lifecycle;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement(name = "NoncurrentVersionExpiration")
@XmlAccessorType(XmlAccessType.FIELD)
public class NoncurrentVersionExpiration extends Condition {
    @XmlElement(name = "NoncurrentDays", required = true)
    private Integer noncurrentDays;

    public NoncurrentVersionExpiration() {
    }

    public NoncurrentVersionExpiration(Integer noncurrentDays) {
        this.noncurrentDays = noncurrentDays;
    }

    public Integer getNoncurrentDays() {
        return noncurrentDays;
    }

    public void setNoncurrentDays(Integer noncurrentDays) {
        this.noncurrentDays = noncurrentDays;
    }

    @Override
    public String toString() {
        return "NoncurrentVersionExpiration{" +
                "noncurrentDays=" + noncurrentDays +
                '}';
    }
}
