package com.macrosan.message.xmlmsg;


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
@XmlRootElement(name = "FsQuotaListConfigResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "FsDirQuotaConfigResult", propOrder = {
        "bucket",
        "listQuotaConfig"
})
public class FsQuotaListConfigResult {

    @XmlElement(name = "Bucket", required = true)
    private String bucket;

    @XmlElement(name = "FsQuotaConfigResult", required = true)
    @XmlElementWrapper(name = "BucketFSQuotaList")
    private List<FsQuotaConfigAndUsedResult> listQuotaConfig;
}
