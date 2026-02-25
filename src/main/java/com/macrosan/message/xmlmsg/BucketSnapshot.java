package com.macrosan.message.xmlmsg;

import com.macrosan.snapshot.enums.BucketSnapshotType;
import lombok.*;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * @author zhaoyang
 * @date 2024/06/24
 **/
@Data
@Accessors(chain = true)
@XmlRootElement(name = "BucketSnapshot")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "BucketSnapshot", propOrder = {
        "mark",
        "name",
        "createTime",
        "parent",
        "deletedParent",
        "children",
        "state",
        "type"
})
public class BucketSnapshot implements Comparable<BucketSnapshot> {
    @XmlElement(name = "Name", required = true)
    private String name;
    @XmlElement(name = "Mark", required = true)
    private String mark;
    @XmlElement(name = "CreateTime", required = true)
    private String createTime;
    @XmlElement(name = "Parent", required = true)
    private String parent;
    @XmlElement(name = "DeletedParent", required = true)
    private String deletedParent;
    @XmlElement(name = "Children", required = true)
    private List<String> children;
    @XmlElement(name = "State", required = true)
    private String state;
    @XmlElement(name = "Type", required = true)
    private BucketSnapshotType type;

    @Override
    public int compareTo(BucketSnapshot o) {
        return this.mark.compareTo(o.mark);
    }
}
