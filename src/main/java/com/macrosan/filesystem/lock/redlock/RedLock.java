package com.macrosan.filesystem.lock.redlock;

import com.macrosan.filesystem.lock.Lock;
import com.macrosan.httpserver.ServerConfig;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@EqualsAndHashCode(callSuper = false, exclude = "lockType")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RedLock extends Lock {
    LockType lockType;
    String value;
    String node;

    @Override
    public boolean needKeep() {
        return node.equals(ServerConfig.getInstance().getHostUuid());
    }
}
