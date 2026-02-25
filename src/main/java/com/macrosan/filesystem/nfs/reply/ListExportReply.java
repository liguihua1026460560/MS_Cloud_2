package com.macrosan.filesystem.nfs.reply;

import com.macrosan.filesystem.nfs.RpcReply;
import com.macrosan.filesystem.nfs.SunRpcHeader;
import com.macrosan.filesystem.utils.FSIPACLUtils;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

@ToString
public class ListExportReply extends RpcReply {
    public ListExportReply(SunRpcHeader header) {
        super(header);
    }

    public int valueFollow = 1;
    public List<ExportEntry> exportEntries = new LinkedList<>();

    @ToString
    public static class ExportEntry {
        public ExportEntry() {

        }

        public ExportEntry(int dirLen, byte[] dirName, int entryFollow, Set<String> ipInfoSet) {
            this.dirLen = dirLen;
            this.dirName = dirName;
            this.entryFollow = entryFollow;
            addIpGroup(ipInfoSet);
        }

        public int dirLen;
        public byte[] dirName;
        public int groupValueFollow = 1;
        public List<IPGroupEntry> ipGroupEntries = new LinkedList<>();
        public int entryFollow;

        public int writeStruct(ByteBuf buf, int offset) {
            int start = offset;
            buf.setInt(offset, dirLen);
            offset += 4;
            buf.setBytes(offset, dirName);
            int fillLen = (dirLen + 3) / 4 * 4;
            offset += fillLen;
            buf.setInt(offset, groupValueFollow);
            offset += 4;
            for (IPGroupEntry entry : ipGroupEntries) {
                offset += entry.writeStruct(buf, offset);
            }
            buf.setInt(offset, entryFollow);
            offset += 4;
            return offset - start;
        }

        public void addIpGroup(Set<String> ipInfoSet) {
            if (ipInfoSet.isEmpty()) {
                this.ipGroupEntries.add(new IPGroupEntry("*".length(), "*".getBytes(), 0));
                return;
            }
            int idx = 0;
            for (String ip : ipInfoSet) {
                boolean needEnd = ++idx == ipInfoSet.size();
                // 172.20.18.1-160 形式
                if (ip.contains("-")) {
                    List<String> allIpFromRange = FSIPACLUtils.getAllIpFromRange(ip);
                    for (int j = 0; j < allIpFromRange.size(); j++) {
                        if (j == allIpFromRange.size() - 1) {
                            this.ipGroupEntries.add(new IPGroupEntry(allIpFromRange.get(j).length(), allIpFromRange.get(j).getBytes(), needEnd ? 0 : 1));
                        } else {
                            this.ipGroupEntries.add(new IPGroupEntry(allIpFromRange.get(j).length(), allIpFromRange.get(j).getBytes(), 1));
                        }
                    }
                }
                //172.20.* 形式
                else if (ip.endsWith("*")) {
                    String s = FSIPACLUtils.convertWildcardToCIDR(ip);
                    this.ipGroupEntries.add(new IPGroupEntry(s.length(), s.getBytes(), needEnd ? 0 : 1));
                }
                // 172.20.18.1 形式， 172.20.0.0/16形式
                else {
                    this.ipGroupEntries.add(new IPGroupEntry(ip.length(), ip.getBytes(), needEnd ? 0 : 1));
                }
            }
        }

        public int getSize() {
            return 12 + (dirLen + 3) / 4 * 4 + ipGroupEntries.stream().mapToInt(IPGroupEntry::getSize).sum();
        }
    }

    @ToString
    public static class IPGroupEntry {
        public int contentLength;
        public byte[] content;
        public int ipGroupFollow;

        public IPGroupEntry(int contentLength, byte[] content, int ipGroupFollow) {
            this.contentLength = contentLength;
            this.content = content;
            this.ipGroupFollow = ipGroupFollow;
        }

        public int writeStruct(ByteBuf buf, int offset) {
            int start = offset;
            buf.setInt(offset, contentLength);
            offset += 4;
            buf.setBytes(offset, content);
            int fillLen = (contentLength + 3) / 4 * 4;
            offset += fillLen;
            buf.setInt(offset, ipGroupFollow);
            offset += 4;
            return offset - start;
        }

        public int getSize() {
            return 8 + (contentLength + 3) / 4 * 4;
        }
    }

    @Override
    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        offset += super.writeStruct(buf, offset);
        buf.setInt(offset, valueFollow);
        offset += 4;
        if (valueFollow == 1) {
            for (ExportEntry entry : exportEntries) {
                offset += entry.writeStruct(buf, offset);
            }
        }

        return offset - start;
    }

    public int getSize() {
        int size = 4;//status长度
        for (ExportEntry entry : exportEntries) {
            size += entry.getSize();
        }
        return size;
    }
}
