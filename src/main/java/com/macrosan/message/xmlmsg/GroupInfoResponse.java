package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.GroupInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * GroupInfoResponse
 * 保存用户组响应信息的容器
 *
 * @author shilinyong
 * @date 2019/08/02
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "GroupInfoResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "GroupInfoResponse", propOrder = {
        "groupInfo"
})
public class GroupInfoResponse {

    @XmlElement(name = "GroupInfo", required = true)
    private GroupInfo groupInfo;
}
