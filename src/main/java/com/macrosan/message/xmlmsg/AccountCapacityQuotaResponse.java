package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.AccountInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;

/**
 * AccountCapacityQuotaResponse
 * 保存账户容量配额信息的容器
 *
 * @author shilinyong
 * @date 2020/03/12
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "AccountCapacityQuotaResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AccountCapacityQuotaResponse", propOrder = {
        "accountInfo"
})
public class AccountCapacityQuotaResponse {

    @XmlElement(name = "AccountInfo", required = true)
    private AccountInfo accountInfo;

}

