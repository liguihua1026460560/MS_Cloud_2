package com.macrosan.message.xmlmsg.Encryption;/**
 * @author niechengxing
 * @create 2023-02-10 10:07
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
 *@create: 2023-02-10 10:07
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "Rule")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "EncryptionRule", propOrder = {
        "applyServerSideEncryptionByDefault"
})
public class Rule {
    @XmlElement(name = "ApplyServerSideEncryptionByDefault")
    private ApplyServerSideEncryptionByDefault applyServerSideEncryptionByDefault;

//    @XmlElement(name = "BucketKeyEnabled")
//    private String bucketKeyEnabled;
}

