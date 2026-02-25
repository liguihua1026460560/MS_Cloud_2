package com.macrosan.filesystem.nfs.types;

import com.macrosan.filesystem.nfs.call.v4.CompoundCall;
import com.macrosan.filesystem.nfs.delegate.DelegateLock;
import com.macrosan.filesystem.nfs.handler.NFSHandler;
import com.macrosan.filesystem.nfs.reply.v4.CompoundReply;
import com.macrosan.utils.functional.Tuple2;
import io.vertx.reactivex.core.net.SocketAddress;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS4ERR_BADSLOT;
import static com.macrosan.filesystem.nfs.NFS.nfsPort;

public class NFS4Session {
    private final byte[] sessionId;
    private final NFS4SessionSlot[] slots;
    private final NFS4Client client;
    private final int maxOps;
    private final int maxCbOps;
    //delegate回调使用
    public NFSHandler nfsHandler;
    public int cbProgram;
    public final AtomicInteger cbSeqId = new AtomicInteger(0);
    private final Set<SessionConnection> boundConnections;
    public final ConcurrentLinkedQueue<Tuple2<CompoundCall, DelegateLock>> recallQueue = new ConcurrentLinkedQueue<>();
    public final AtomicBoolean canSend  = new AtomicBoolean(true);


    public NFS4Session(NFS4Client client, byte[] sessionId, int replyCacheSize, int maxOps, int maxCbOps, NFSHandler nfsHandler, int cbProgram) {
        this.client = client;
        slots = new NFS4SessionSlot[replyCacheSize];
        this.sessionId = sessionId;
        this.maxOps = maxOps;
        this.maxCbOps = maxCbOps;
//        this.cbReplyCacheSize = cbReplyCacheSize;
        boundConnections = new HashSet<>();
        this.nfsHandler = nfsHandler;
        this.cbProgram = cbProgram;
    }

    public byte[] getSessionId() {
        return sessionId;
    }

    public NFS4Client getClient() {
        return client;
    }

    public int getMaxOps() {
        return maxOps;
    }


    public int getMaxCbOps() {
        return maxCbOps;
    }


    public int getHighestSlot() {
        return slots.length - 1;
    }


    public NFS4SessionSlot getSessionSlot(int slot, CompoundReply reply) {
        if (slot < 0 || slot > getHighestSlot()) {
            reply.status = NFS4ERR_BADSLOT;
            return null;
        }

        if (slots[slot] == null) {
            slots[slot] = new NFS4SessionSlot();
        }

        return slots[slot];
    }


    public synchronized void bindIfNeeded(SessionConnection connection) {
        if (boundConnections.isEmpty()) {
            bindToConnection(connection);
        }
    }


    public synchronized void bindToConnection(SessionConnection connection) {
        boundConnections.add(connection);
    }

    public synchronized boolean isReleasableBy(SessionConnection connection) {
        return boundConnections.isEmpty() || boundConnections.contains(connection);
    }

    @AllArgsConstructor
    @Data
    public static class Session {
        public byte[] session;
    }

    public static class SessionConnection {

        private final SocketAddress local;
        private final SocketAddress remote;
        public static SocketAddress DEFAULT_ADDRESS = new SocketAddress(new io.vertx.core.net.SocketAddress() {
            @Override
            public String host() {
                return "127.0.0.1";
            }

            @Override
            public int port() {
                return nfsPort;
            }

            @Override
            public String path() {
                return "/";
            }
        });

        public SessionConnection(SocketAddress local, SocketAddress remote) {
            this.local = local;
            this.remote = remote;
        }

        @Override
        public int hashCode() {
            return Objects.hash(local, remote);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final SessionConnection other = (SessionConnection) obj;
            return Objects.equals(local, other.local) && Objects.equals(remote, other.remote);
        }

        public SocketAddress getLocalConnection() {
            return local;
        }

        public SocketAddress getRemoteConnection() {
            return remote;
        }

    }
}
