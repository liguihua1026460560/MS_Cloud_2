package com.macrosan.message.xmlmsg.notification;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "Filter")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Filter",propOrder = {
        "s3Key"
})
public class Filter {
    @XmlElement(name = "S3Key")
    private S3Key s3Key;
}
