package com.macrosan.filesystem.cifs.types;

import lombok.Data;

@Data
public class NegprotInfo {
    short dialect;
    int flags;
}
