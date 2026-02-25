package com.macrosan.message.xmlmsg.referer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "RefererConfiguration")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "RefererConfiguration", propOrder = {

})
public class RefererConfiguration {

    @XmlElement(name = "AntiLeechEnabled")
    private String antiLeechEnabled;

    @XmlElement(name = "AllowEmptyReferer")
    private String allowEmptyReferer;

    @XmlElement(name = "BlackRefererList")
    private BlackRefererList blackRefererList;

    @XmlElement(name = "WhiteRefererList")
    private WhiteRefererList whiteRefererList;
}
