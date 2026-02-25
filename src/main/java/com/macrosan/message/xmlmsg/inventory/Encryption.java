package com.macrosan.message.xmlmsg.inventory;

import lombok.Data;

import javax.xml.bind.annotation.*;

/**
 * <p></p>
 *
 * @author Administrator
 * @version 1.0
 * @className Encryption
 * @date 2022/6/15 14:22
 */
@Data
@XmlRootElement(name = "Encryption")
@XmlAccessorType(XmlAccessType.FIELD)
public class Encryption {

    @XmlElement(name = "SSE-KMS")
    private SSEKMS ssekms;

    @XmlElement(name = "SSES3", required = false)
    private SSES3 sses3;

}
