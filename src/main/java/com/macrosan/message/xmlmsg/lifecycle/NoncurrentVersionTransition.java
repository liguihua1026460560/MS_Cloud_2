package com.macrosan.message.xmlmsg.lifecycle;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement(name = "NoncurrentVersionTransition")
@XmlAccessorType(XmlAccessType.FIELD)
public class NoncurrentVersionTransition extends Condition {
    @XmlElement(name = "NoncurrentDays", required = true)
    private Integer noncurrentDays;

    @XmlElement(name = "StorageClass", required = true)
    private String storageClass;

    public NoncurrentVersionTransition() {
    }

    public NoncurrentVersionTransition(Integer noncurrentDays, String storageClass) {
        this.noncurrentDays = noncurrentDays;
        this.storageClass = storageClass;
    }

    public Integer getNoncurrentDays() {
        return noncurrentDays;
    }

    public void setNoncurrentDays(Integer noncurrentDays) {
        this.noncurrentDays = noncurrentDays;
    }

    public String getStorageClass() {
        return storageClass;
    }

    public void setStorageClass(String storageClass) {
        this.storageClass = storageClass;
    }

    @Override
    public String toString() {
        return "NoncurrentVersionTransition{" +
                "noncurrentDays=" + noncurrentDays +
                ", storageClass='" + storageClass + '\'' +
                '}';
    }
}
