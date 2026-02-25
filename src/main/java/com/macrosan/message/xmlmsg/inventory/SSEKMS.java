package com.macrosan.message.xmlmsg.inventory;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * <p></p>
 *
 * @author Administrator
 * @version 1.0
 * @className SSEKMS
 * @date 2022/6/15 14:27
 */
@XmlRootElement(name = "SSEKMS")
@XmlAccessorType(XmlAccessType.FIELD)
public class SSEKMS {

    @XmlElement(name = "KeyId")
    private String keyId;

}
