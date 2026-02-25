package com.macrosan.filesystem.nfs.types;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString
public class Owner {

    @EqualsAndHashCode.Include
    public String ip;
    @EqualsAndHashCode.Include
    public String clientName;
    @EqualsAndHashCode.Include
    public int svid;
    @EqualsAndHashCode.Include
    public long ino;
    @EqualsAndHashCode.Include
    public String local;

    public boolean exclusive;
    @EqualsAndHashCode.Include
    public long offset = 0;
    @EqualsAndHashCode.Include
    public long len = 0;

    public String node;

    public Owner() {
    }

    public Owner(String ip, String clientName, int svid, long ino, String local) {
        this.ip = ip;
        this.clientName = clientName;
        this.svid = svid;
        this.ino = ino;
        this.local = local;
    }

    public Owner(String ip, String clientName, int svid, long ino, String local, long offset, long len) {
        this.ip = ip;
        this.clientName = clientName;
        this.svid = svid;
        this.ino = ino;
        this.local = local;
        this.offset = offset;
        this.len = len;
    }

    public String mergeKey() {
        return this.ip + " " + this.local + "/" + this.svid + "/0-0";
    }

    public String ownerKey() {
        return this.ip + " " + this.local + "/" + this.svid + "/" + this.offset + "-" + this.len;
    }

    public String info() {
        return "Owner{" +
                "ip='" + ip + '\'' +
                ", clientName='" + clientName + '\'' +
                ", svid=" + svid +
                ", ino=" + ino +
                ", local='" + local + '\'' +
                ", exclusive=" + exclusive +
                ", offset=" + offset +
                ", len=" + len +
                '}';
    }
}
