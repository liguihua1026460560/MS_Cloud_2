package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * AccountInfo
 * 保存账户信息的容器
 *
 * @author shilinyong
 * @date 2020/03/12
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "AccountInfos")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AccountInfos", propOrder = {
        "accountInfo",
})
public class AccountInfos {

    @XmlElement(name = "AccountInfo", required = true)
    private List<AccountInfo> accountInfo;
}
