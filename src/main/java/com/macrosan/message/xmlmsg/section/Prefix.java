package com.macrosan.message.xmlmsg.section;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Contents
 * 保存返回结果中每个 Object 信息的容器
 *
 * @author liyixin
 * @date 2018/11/12
 */
@Data
@Accessors(chain = true)
@XmlRootElement(name = "CommonPrefixes")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CommonPrefixes", propOrder = {
        "prefix"
})
public class Prefix {

    @XmlElement(name = "Prefix", required = true)
    private String prefix;

}
