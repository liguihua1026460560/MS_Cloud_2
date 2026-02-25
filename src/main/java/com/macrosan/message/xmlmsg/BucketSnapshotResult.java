package com.macrosan.message.xmlmsg;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;
import java.util.Set;

/**
 * @author zhaoyang
 * @date 2024/06/26
 **/
@Data
@Accessors(chain = true)
@XmlRootElement(name = "BucketSnapshotResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "BucketSnapshotResult", propOrder = {
        "bucketSnapshots"
})
public class BucketSnapshotResult {
    @XmlElement(name = "BucketSnapshot")
    private Set<BucketSnapshot> bucketSnapshots;
}
