package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * GroupInfo
 * 保存用户组信息的容器
 *
 * @author shilinyong
 * @date 2019/08/02
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "GroupInfos")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "GroupInfos", propOrder = {
        "groupInfo",
})
public class GroupInfos {

    @XmlElement(name = "GroupInfo", required = true)
    private List<GroupInfo> groupInfo;
}
