package com.macrosan.inventory.transmitter;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Destination {
    private String bucket;
    private String object;
    private String versionId;
    private String timestamp;
    private String fileName;
}
