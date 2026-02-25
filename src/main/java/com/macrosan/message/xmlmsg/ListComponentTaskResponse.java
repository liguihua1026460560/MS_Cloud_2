package com.macrosan.message.xmlmsg;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * @author zhaoyang
 * @date 2023/07/25
 **/

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ComponentTasks")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ComponentTasks", propOrder = {
        "componentTaskList",
})
public class ListComponentTaskResponse {
    @XmlElementWrapper(name = "ComponentTaskList")
    @XmlElement(name = "ComponentTask", required = true)
    private List<ComponentTask> componentTaskList;
}
