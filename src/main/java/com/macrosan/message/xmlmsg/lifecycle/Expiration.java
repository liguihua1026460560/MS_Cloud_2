package com.macrosan.message.xmlmsg.lifecycle;


import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement(name = "Expiration")
@XmlAccessorType(XmlAccessType.FIELD)
public class Expiration extends Condition {
    public static final String OPERATION = "expiration";
    @XmlElement(name = "Days", required = false)
    private Integer days;

    @XmlElement(name = "Date", required = false)
    private String date;

    public Expiration() {

    }

    public Expiration(Integer days, String date) {
        this.days = days;
        this.date = date;
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

    @Override
    public String toString() {
        return "Expiration{" +
                "days=" + days +
                ", date='" + date + '\'' +
                '}';
    }
}
