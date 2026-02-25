package com.macrosan.message.xmlmsg.worm;

import lombok.Data;

import javax.xml.bind.annotation.*;

/**
 * <p></p>
 *
 * @author Administrator
 * @version 1.0
 * @className Rule
 * @date 2021/9/6 14:26
 */
@Data
@XmlRootElement(name = "Rule")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Rule", propOrder = {
        "defaultRetention"
})
public class Rule {

    @XmlElement(name = "DefaultRetention", required = true)
    private DefaultRetention defaultRetention;

}
