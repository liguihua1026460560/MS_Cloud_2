package com.macrosan.message.xmlmsg.Encryption;/**
 * @author niechengxing
 * @create 2023-02-10 9:59
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
 *@create: 2023-02-10 09:59
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "ServerSideEncryptionConfiguration")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ServerSideEncryptionConfiguration", propOrder = {
        "rule"
})
public class ServerSideEncryptionConfiguration {
    @XmlElement(name = "Rule",required = true)
    private Rule rule;
}

