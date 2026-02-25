package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.AccountInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * <p></p>
 *
 * @author Administrator
 * @version 1.0
 * @className AccountObjectsQuotaResponse
 * @date 2021/10/8 14:17
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "AccountObjectsQuotaResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AccountObjectsQuotaResponse", propOrder = {
        "accountInfo"
})
public class AccountObjectsQuotaResponse {
    @XmlElement(name = "AccountInfo", required = true)
    private AccountInfo accountInfo;
}
