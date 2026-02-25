package com.macrosan.message.xmlmsg.lifecycle;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement(name = "Transition")
@XmlAccessorType(XmlAccessType.FIELD)
public class Transition extends Condition {
    @XmlElement(name = "Days", required = false)
    private Integer days;

    @XmlElement(name = "Date", required = false)
    private String date;

    @XmlElement(name = "StorageClass", required = false)
    private String storageClass;

    public Transition() {
    }

    public Transition(Integer days, String date, String storageClass) {
        this.days = days;
        this.date = date;
        this.storageClass = storageClass;
    }

    public Integer getDays() {
        return days;
    }

    public void setDays(Integer days) {
        this.days = days;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getStorageClass() {
        return storageClass;
    }

    public void setStorageClass(String storageClass) {
        this.storageClass = storageClass;
    }

    @Override
    public String toString() {
        return "Transition{" +
                "days=" + days +
                ", date='" + date + '\'' +
                ", storageClass='" + storageClass + '\'' +
                '}';
    }
}
