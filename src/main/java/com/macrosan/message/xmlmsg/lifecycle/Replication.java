package com.macrosan.message.xmlmsg.lifecycle;

import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@EqualsAndHashCode(callSuper = true)
@XmlRootElement(name = "Replication")
@XmlAccessorType(XmlAccessType.FIELD)
@Data
public class Replication extends Condition {
    @XmlElement(name = "Days", required = false)
    private Integer days;

    @XmlElement(name = "Date", required = false)
    private String date;

    @XmlElement(name = "SourceSite", required = false)
    private String sourceSite;

    @XmlElement(name = "BackUpSite", required = false)
    private String backupSite;

    public Replication() {
    }

    public Replication(Integer days, String date, String backupSite) {
        this.days = days;
        this.date = date;
        this.backupSite = backupSite;
    }
}
