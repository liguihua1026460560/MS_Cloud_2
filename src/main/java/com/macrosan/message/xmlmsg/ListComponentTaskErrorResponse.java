package com.macrosan.message.xmlmsg;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * @author zhaoyang
 * @date 2023/11/23
 **/

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListComponentErrorResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListComponentErrorResult", propOrder = {
        "componentErrorResponseList",
})
public class ListComponentTaskErrorResponse {

    @XmlElementWrapper(name = "ComponentTaskErrorList")
    @XmlElement(name = "ComponentTaskError")
    List<ComponentTaskErrorResponse> componentErrorResponseList;

}
