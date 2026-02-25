package com.macrosan.message.xmlmsg;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * @author zhaoyang
 * @date 2023/12/08
 **/
@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListComponentObjectErrorResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListComponentObjectErrorResult", propOrder = {
        "componentObjectErrorResponses"
})
public class ListComponentObjectErrorResponse {

    @XmlElementWrapper(name = "ComponentObjectErrorList")
    @XmlElement(name = "ComponentObjectError")
    List<ComponentObjectErrorResponse> componentObjectErrorResponses;

}
