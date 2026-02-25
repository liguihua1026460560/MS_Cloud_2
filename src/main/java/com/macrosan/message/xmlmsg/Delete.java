package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.ObjectsList;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * Delete
 * 保存批量删除成功对象列表的容器
 *
 * @author zhoupeng
 * @date 2019/7/26
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "Delete")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(namespace = "Delete", propOrder = {
        "quiet",
        "objects"
})
public class Delete {
    @XmlElement(name = "Quiet", required = false)
    private String quiet;

    @XmlElement(name = "Object", required = true)
    private List<ObjectsList> objects;
}
