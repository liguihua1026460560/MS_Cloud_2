package com.macrosan.message.xmlmsg.lifecycle;/**
 * @author niechengxing
 * @create 2024-01-22 11:31
 */

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *@program: MS_Cloud
 *@description:
 *@author: niechengxing
 *@create: 2024-01-22 11:31
 */

@XmlRootElement(name = "Tag")
@XmlAccessorType(XmlAccessType.FIELD)
public class Tag {
    @XmlElement(name = "Key", required = true)
    private String key;

    @XmlElement(name = "Value", required = true)
    private String value;

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Tag() {
    }

    @Override
    public String toString() {
        return "Tag{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}

