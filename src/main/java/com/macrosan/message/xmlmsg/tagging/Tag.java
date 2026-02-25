package com.macrosan.message.xmlmsg.tagging;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

@Data
@XmlAccessorType(XmlAccessType.FIELD)
@AllArgsConstructor
@NoArgsConstructor
@XmlType(name = "Tag", propOrder = {
        "key",
        "value"
})
public class Tag {
    @XmlElement(name = "Key")
    private String key;
    @XmlElement(name = "Value")
    private String value;
}
