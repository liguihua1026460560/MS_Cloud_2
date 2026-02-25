package com.macrosan.filesystem.cifs.notify;

import com.macrosan.filesystem.cifs.reply.smb2.NotifyReply;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class NotifyTask {
    String bucket;
    String[] notifyKey;
    int filter;
    NotifyReply.NotifyAction[] action;
}
