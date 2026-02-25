package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * swift访问时保存多个bucket信息的容器
 */

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "Containers")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Containers", propOrder = {
        "name",
        "creation_date"
})
public class BucketSwift {

    @XmlElement(name = "name", required = true)
    private String name;

    @XmlElement(name = "creation_date", required = true)
    private String creation_date;
}
