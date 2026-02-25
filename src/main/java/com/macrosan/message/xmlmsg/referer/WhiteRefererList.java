package com.macrosan.message.xmlmsg.referer;

import lombok.Data;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@Data
@XmlRootElement(name = "BlackRefererList")
@XmlAccessorType(XmlAccessType.FIELD)
public class WhiteRefererList {
    @XmlElement(name = "Referer")
    public List<String> Referer;
}
