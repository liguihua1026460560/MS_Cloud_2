package com.macrosan.message.xmlmsg;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "BucketLoggingStatus")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "BucketLoggingStatus", propOrder = {
        "logEnabled",
})
public class BucketLoggingStatus {
    @XmlElement(name = "LoggingEnabled" , required = true)
    private LoggingEnabled logEnabled;

}



