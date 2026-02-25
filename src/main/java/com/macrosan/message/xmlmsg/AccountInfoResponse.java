package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.AccountInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * AccountInfoResponse
 * 保存账户响应信息的容器
 *
 * @author shilinyong
 * @date 2020/03/12
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "AccountInfoResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AccountInfoResponse", propOrder = {
        "accountInfo"
})
public class AccountInfoResponse {

    @XmlElement(name = "AccountInfo", required = true)
    private AccountInfo accountInfo;
}
