package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Grant
 * 标记用户和用户的权限
 *
 * @author liyixin
 * @date 2018/11/12
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "Grant")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Grant", propOrder = {
        "grantee",
        "permission"
})
public class Grant {

    @XmlElement(name = "Grantee", required = true)
    private Grantee grantee;

    @XmlElement(name = "Permission", required = true)
    private String permission;
}

