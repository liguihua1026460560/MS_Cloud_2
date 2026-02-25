package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Grantee
 * 保存被授权者信息的容器
 *
 * @author liyixin
 * @date 2018/11/12
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(namespace = "http://www.w3.org/2001/XMLSchema-instance")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Grantee", propOrder = {
        "id",
        "displayName"
})
public class Grantee {

    @XmlElement(name = "ID", required = true)
    private String id;

    @XmlElement(name = "DisplayName", required = true)
    private String displayName;

    @XmlAttribute(namespace = "http://www.w3.org/2001/XMLSchema-instance")
    private static final String TYPE = "CanonicalUser";
}