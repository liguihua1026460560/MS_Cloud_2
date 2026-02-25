package com.macrosan.message.xmlmsg.Encryption;/**
 * @author niechengxing
 * @create 2023-02-10 10:12
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 *@program: MS_Cloud
 *@description:
 *@author: niechengxing
 *@create: 2023-02-10 10:12
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "ApplyServerSideEncryptionByDefault")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ApplyServerSideEncryptionByDefault", propOrder = {
        "SSEAlgorithm"
})
public class ApplyServerSideEncryptionByDefault {
    @XmlElement(name = "SSEAlgorithm",required = true)
    private String SSEAlgorithm;

//    @XmlElement(name = "KMSMasterKeyID")
//    private String KMSMasterKeyID;
}

