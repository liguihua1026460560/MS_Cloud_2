package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.Deleted;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * 批量删除结果XML
 *
 * @author zhoupeng
 * @date 2019/7/26
 */
@Data
@Accessors(chain = true)
@XmlRootElement(name = "DeleteResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DeleteResult", propOrder = {
        "deleted",
        "errors",
})

public class DeleteMultipleObjects {
    @XmlElement(name = "Deleted", required = false)
    private List<Deleted> deleted;

    @XmlElement(name = "Error", required = false)
    private List<Error> errors;
}