package com.macrosan.message.xmlmsg.region;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;


/**
 * 创建桶时配置该桶的区域信息
 * @author chengyinfeng
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "CreateBucketConfiguration")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CreateBucketConfiguration", propOrder = {
        "locationConstraint"
})
public class CreateBucketConfiguration {

    @XmlElement(name = "LocationConstraint", required = true)
    private String locationConstraint;

}
