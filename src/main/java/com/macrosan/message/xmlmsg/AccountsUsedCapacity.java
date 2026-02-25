package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.AccountUsedCapacity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * 保存账户已使用容量的容器
 *
 * @author sunfang
 * @date 2019/7/1
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "AccountsUsedCapacity")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AccountsUsedCapacity", propOrder = {
        "resData"
})
public class AccountsUsedCapacity {

    @XmlElement(name = "AccountUsedCapacity", required = true)
    private List<AccountUsedCapacity> resData;

}