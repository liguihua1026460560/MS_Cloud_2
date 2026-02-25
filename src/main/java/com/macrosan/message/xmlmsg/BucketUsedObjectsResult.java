package com.macrosan.message.xmlmsg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * <p></p>
 *
 * @author Administrator
 * @version 1.0
 * @className BucketUsedObjectsResult
 * @date 2021/10/19 8:51
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "BucketUsedObjectsResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "BucketUsedObjectsResult", propOrder = {
        "usedObjects",
})
public class BucketUsedObjectsResult {
    @XmlElement(name = "UsedObjects", required = true)
    private String usedObjects;
}
