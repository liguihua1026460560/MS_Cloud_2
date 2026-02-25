package com.macrosan.message.xmlmsg.notification;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "Events")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Events",propOrder = {
        "event"
})
public class Events {
    @XmlElement(name = "Event")
    private List<String> event;
}
