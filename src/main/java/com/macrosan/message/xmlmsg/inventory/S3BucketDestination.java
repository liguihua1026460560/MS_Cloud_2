package com.macrosan.message.xmlmsg.inventory;

import lombok.Data;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * <p></p>
 *
 * @author Administrator
 * @version 1.0
 * @className S3BucketDestination
 * @date 2022/6/15 14:16
 */
@Data
@XmlRootElement(name = "S3BucketDestination")
@XmlAccessorType(XmlAccessType.FIELD)
public class S3BucketDestination {

    @XmlElement(name = "AccountId")
    private String accountId;

    @XmlElement(name = "Bucket", required = true)
    private String bucket;

    @XmlElement(name = "Format")
    private String format;

    @XmlElement(name = "Prefix")
    private String prefix;

    @XmlElement(name = "Encryption")
    private Encryption encryption;

}
