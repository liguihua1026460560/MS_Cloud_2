package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.AccountInfos;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * AccountInfosResponse
 * 保存账户列表响应信息的容器
 *
 * @author shilinyong
 * @date 2020/03/12
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "AccountInfosResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AccountInfosResponse", propOrder = {
        "accountInfos"
})
public class AccountInfosResponse {

    @XmlElement(name = "AccountInfos", required = true)
    private AccountInfos accountInfos;
}
