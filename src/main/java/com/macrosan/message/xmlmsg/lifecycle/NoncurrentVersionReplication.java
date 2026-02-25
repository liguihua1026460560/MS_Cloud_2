package com.macrosan.message.xmlmsg.lifecycle;

import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;


@EqualsAndHashCode(callSuper = true)
@XmlRootElement(name = "NoncurrentVersionReplication")
@XmlAccessorType(XmlAccessType.FIELD)
@Data
public class NoncurrentVersionReplication extends Condition {
    @XmlElement(name = "NoncurrentDays", required = true)
    private Integer noncurrentDays;

    @XmlElement(name = "SourceSite", required = false)
    private String sourceSite;

    @XmlElement(name = "BackUpSite", required = false)
    private String backupSite;

    public NoncurrentVersionReplication() {
    }

    public NoncurrentVersionReplication(Integer noncurrentDays, String sourceSite, String backupSite) {
        this.noncurrentDays = noncurrentDays;
        this.sourceSite = sourceSite;
        this.backupSite = backupSite;
    }
}
