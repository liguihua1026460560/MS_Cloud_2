package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.GroupInfos;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * GroupInfosResponse
 * 保存用户组列表响应信息的容器
 *
 * @author shilinyong
 * @date 2019/08/02
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "GroupInfosResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "GroupInfosResponse", propOrder = {
        "groupInfos"
})
public class GroupInfosResponse {

    @XmlElement(name = "GroupInfos", required = true)
    private GroupInfos groupInfos;
}
