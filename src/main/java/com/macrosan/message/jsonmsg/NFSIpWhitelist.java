package com.macrosan.message.jsonmsg;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
@EqualsAndHashCode
public class NFSIpWhitelist {
    public String ip;
    public int ipType; // 1 ipv4, 2 ipv6(暂不支持)
    public String mask;

    public NFSIpWhitelist() {
    }

    public NFSIpWhitelist(String ip, int ipType, String mask) {
        this.ip = ip;
        this.ipType = ipType;
        this.mask = mask;
    }
}
