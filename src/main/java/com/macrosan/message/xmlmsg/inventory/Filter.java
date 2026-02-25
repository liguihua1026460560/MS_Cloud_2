package com.macrosan.message.xmlmsg.inventory;

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
 * @className Filter
 * @date 2022/6/15 14:00
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
@XmlRootElement(name = "Filter")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "inventory.Filter", propOrder = {
        "prefix"
})
public class Filter {

    @XmlElement(name = "Prefix")
    private String prefix;

}
