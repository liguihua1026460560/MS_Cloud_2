package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.Grant;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "TargetGrants")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "TargetGrants", propOrder = {
        "grant"
})
public class TargetGrants {
    @XmlElement(name = "Grant",required = true)
    Grant grant;
}
