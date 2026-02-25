package com.macrosan.message.xmlmsg.cors;

import lombok.Data;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.*;

@Data
@XmlRootElement(name = "CORSRule")
@XmlAccessorType(XmlAccessType.FIELD)
public class CORSRule {

    @XmlElement(name = "AllowedOrigin", required = true)
    private Set<String> allowedOrigins;

    @XmlElement(name = "AllowedMethod", required = true)
    private Set<String> allowedMethods;

    @XmlElement(name = "AllowedHeader")
    private Set<String> allowedHeaders;

    @XmlElement(name = "ExposeHeader")
    private Set<String> exposeHeaders;

    @XmlElement(name = "MaxAgeSeconds")
    private String maxAgeSeconds;

    public CORSRule() {
        allowedOrigins = new LinkedHashSet<>();
        allowedMethods = new LinkedHashSet<>();
        allowedHeaders = new LinkedHashSet<>();
        exposeHeaders = new LinkedHashSet<>();
    }
}
