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
 * @className Destination
 * @date 2022/6/15 13:58
 */
@Data
@XmlRootElement(name = "Destination")
@XmlAccessorType(XmlAccessType.FIELD)
public class Destination {

    @XmlElement(name = "S3BucketDestination")
    private S3BucketDestination s3BucketDestination;

}
