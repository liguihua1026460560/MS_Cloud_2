
package com.macrosan.filesystem.nfs.types;

import com.macrosan.filesystem.nfs.reply.v4.CompoundReply;
import lombok.extern.log4j.Log4j2;

import java.util.Collections;
import java.util.List;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS4ERR_SEQ_MISORDERED;

@Log4j2
public class NFS4SessionSlot {
    //todo slot缓存未实现
    private int sequence;
    private List<CompoundReply> reply;

    public NFS4SessionSlot() {
       sequence = 0;
    }


    List<CompoundReply> acquire(int sequence,CompoundReply reply) {
        if( sequence == this.sequence) {
            log.info("retransmit detected");
            if( this.reply != null ) {
                return this.reply;
            }

            return Collections.emptyList();
        }

        int validValue = this.sequence + 1;
        if (sequence != validValue) {
            reply.status = NFS4ERR_SEQ_MISORDERED;
        }

        this.sequence = sequence;
        this.reply = null;
        return null;
    }

    void update(List<CompoundReply> reply) {
        this.reply = reply;
    }
}
