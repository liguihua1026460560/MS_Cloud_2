package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.Bucket;
import com.macrosan.message.xmlmsg.section.Owner;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * Buckets
 * 保存 GET Service 请求结果的容器
 *
 * @author liyixin
 * @date 2018/11/12
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "ListAllMyBucketsResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Buckets", propOrder = {
        "owners",
        "bucketList"
})
public class Buckets {

    @XmlElement(name = "Owner", required = true)
    private Owner owners;

    @XmlElement(name = "Bucket", required = true)
    @XmlElementWrapper(name = "Buckets")
    private List<Bucket> bucketList;
}
