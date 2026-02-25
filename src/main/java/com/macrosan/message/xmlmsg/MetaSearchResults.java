package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.MetaSearchResult;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

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
@XmlType(name = "MetaSearchResults", propOrder = {
        "Count",
        "resData"
})
public class MetaSearchResults {

    @XmlElement(name = "Count", required = true)
    private int Count;


    @XmlElement(name = "MetaSearchResult", required = true)
    @XmlElementWrapper(name = "MetaSearchResults")
    private List<MetaSearchResult> resData;


}
