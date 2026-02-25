package com.macrosan.filesystem.nfs.types;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class CBInfo {
    public int rpcVersion;
    public int program;
    public int programVersion;
    public int opt;
}
