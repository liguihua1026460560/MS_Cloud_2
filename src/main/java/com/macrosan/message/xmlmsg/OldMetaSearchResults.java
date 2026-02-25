package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.MetaSearchResult;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.jws.WebMethod;
import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * @author Administrator
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "MetaSearchResults")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "MetaSearchResults", namespace = "message", propOrder = {
        "Count",
        "NextNumber",
        "resData"
})
public class OldMetaSearchResults {

    @XmlElement(name = "Count", required = true)
    private int Count;

    @XmlElement(name = "NextNumber", required = true)
    private int NextNumber;

    @XmlElement(name = "MetaSearchResult", required = true)
    @XmlElementWrapper(name = "MetaSearchResults")
    private List<MetaSearchResult> resData;


}
