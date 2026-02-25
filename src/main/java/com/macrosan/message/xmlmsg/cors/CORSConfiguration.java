package com.macrosan.message.xmlmsg.cors;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;
@Data
@Accessors(chain = true)
@XmlRootElement(name = "CORSConfiguration")
@XmlAccessorType(XmlAccessType.FIELD)
public class CORSConfiguration {

    @XmlElement(name = "CORSRule", required = true)
    private List<CORSRule> corsRules;

    @XmlElement(name = "ResponseVary")
    private String responseVary;

    public CORSConfiguration() {
        corsRules = new ArrayList<>();
    }
}
