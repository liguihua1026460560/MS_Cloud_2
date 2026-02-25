package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.Part;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * CompleteMultipartUpload
 * 保存整个访问控制列表的容器
 *
 * @author liyixin
 * @date 2018/11/12
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "CompleteMultipartUpload")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CompleteMultipartUpload", propOrder = {
        "parts"
})
public class CompleteMultipartUpload {

    @XmlElement(name = "Part", required = true)
    private List<Part> parts;

}
