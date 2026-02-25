package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.AccountInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
/**
 * AccountInfosResponse
 * 保存新建账户响应信息的容器
 *
 * @author zhoupeng
 * @date 2020/06/23
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "CreateAccountResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CreateAccountResponse", propOrder = {
        "accountInfo"
})
public class CreateAccountResponse {
    @XmlElement(name = "AccountInfo", required = true)
    private AccountInfo accountInfo;
}
