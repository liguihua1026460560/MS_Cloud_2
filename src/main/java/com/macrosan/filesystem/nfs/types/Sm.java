package com.macrosan.filesystem.nfs.types;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static com.macrosan.filesystem.nfs.lock.NSMServer.PREFIX_SM;
import static com.macrosan.filesystem.nfs.lock.NSMServer.PREFIX_SM_BAK;

@Data
@EqualsAndHashCode(exclude = {"bucket"})
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class Sm {
    public String ip;
    public String clientName;
    public int svid;

    public String bucket;
    public long ino;
//    public String value;

    public String local;
    public long offset;
    public long len;

    public Sm() {
    }

    public Sm(String bucket, Owner owner) {
        this.ip = owner.ip;
        this.clientName = owner.clientName;
        this.svid = owner.svid;
        this.bucket = bucket;
        this.ino = owner.ino;
        this.local = owner.local;
        this.offset = owner.offset;
        this.len = owner.len;
    }

    public Sm(String ip, String clientName, int svid, String bucket, long ino, String local, long offset, long len) {
        this.ip = ip;
        this.clientName = clientName;
        this.svid = svid;
        this.bucket = bucket;
        this.ino = ino;
        this.local = local;
        this.offset = offset;
        this.len = len;
    }

    public String smKey() {
        return PREFIX_SM + this.ip + " " + this.local + "/" + this.ino + "/" + this.svid + "/" + this.offset + "-" + this.len;
    }

    public String smBakKey() {
        return PREFIX_SM_BAK + this.ip + " " + this.local + "/" + this.ino + "/" + this.svid + "/" + this.offset + "-" + this.len;
    }

    public String mergeKey() {
        return this.ip + " " + this.local + "/" + this.svid;
    }

}
