package com.macrosan.message.xmlmsg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * 设置与获取多版本状态XML
 * @author zhoupeng
 * @date 2019.08.30
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "VersioningConfiguration")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(namespace = "VersioningConfiguration", propOrder = {
        "status",
        "mfadelete"
})

public class BucketVersioning {
    @XmlElement(name = "Status", required = true)
    private String status;

    @XmlElement(name = "MfaDelete", required = false)
    private String mfadelete;
}
