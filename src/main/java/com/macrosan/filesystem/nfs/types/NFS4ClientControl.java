
package com.macrosan.filesystem.nfs.types;

import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.filesystem.nfs.auth.Auth;
import com.macrosan.filesystem.nfs.handler.NFSHandler;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import io.vertx.reactivex.core.net.SocketAddress;
import lombok.extern.log4j.Log4j2;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.*;
import static com.macrosan.filesystem.utils.Nfs4Utils.*;
@Log4j2
public class NFS4ClientControl {
    private final static int STATE_INITIAL_SEQUENCE = 0;
    private final AtomicInteger clientId = new AtomicInteger(0);
    private int leaseTime;
    private int instanceId;
    //    private final ClientOps clientOps;
    private Map<Long, NFS4Client> clientMap;
    public static final MsExecutor executor = new MsExecutor(2, 2, new MsThreadFactory("clean-expire"));
    public static final Scheduler CLEAN_SCHEDULER = Schedulers.fromExecutor(executor);

    private final StateIdOps stateIdOps = new StateIdOps();

    public NFS4ClientControl(int leaseTime, String node) {
        try {
            this.leaseTime = leaseTime;
            this.instanceId = Integer.parseInt(node);
            this.clientMap = new ConcurrentHashMap<>();
        } catch (Exception e) {
            this.leaseTime = leaseTime;
            this.instanceId = 0;
            this.clientMap = new ConcurrentHashMap<>();
        } finally {
            tryClearCache();
        }

    }

    public void tryClearCache() {
        try {
            for (Long key : clientMap.keySet()) {
                clientMap.computeIfPresent(key, (k, client) -> {
                    if (!client.leaseValid()) {
                        client.tryDispose().subscribe();
//                        clientOps.removeClient(client.getOwnerId());
                        return null;
                    }
                    return client;
                });
            }
        } finally {
            CLEAN_SCHEDULER.schedule(this::tryClearCache, 10, TimeUnit.SECONDS);
        }
    }

    public void removeClient(NFS4Client client) {
        clientMap.compute(client.getClientId(), (k, v) -> {
            if (v != null) {
                //移除客户端对应的stateId以及lock
                client.tryDispose().subscribe();
            }
            return null;
        });
    }

    private void addClient(NFS4Client newClient) {
        clientMap.put(newClient.getClientId(), newClient);
//        clientOps.addClient(newClient.getOwnerId());
    }


    public NFS4Client getConfirmedClient(long clientId) {
        NFS4Client client = getValidClient(clientId);
        if (!client.getConfirmed()) {
            throw new NFSException(NFS4ERR_STALE_CLIENTID, "getConfirmedClient : client  not confirm");
        }
        return client;
    }


    public NFS4Client getValidClient(long clientId) {
        NFS4Client client = getClient(clientId);
        if (!client.leaseValid()) {
            throw new NFSException(NFS4ERR_STALE_CLIENTID, "getValidClient : client lease not valid");
        }
        return client;
    }


    public StateIdOps getStateIdOps() {
        return stateIdOps;
    }

    public boolean hasClient(long clientId) {
        return clientMap.get(clientId) != null;
    }

    public NFS4Client getClient(long clientId) {
        return clientMap.compute(clientId, (k, v) -> {
            if (v == null) {
                throw new NFSException(NFS4ERR_STALE_CLIENTID, "getClient : not exist clientId");
            }
            return v;
        });
    }

    public NFS4Client getClient0(long clientId) {
        return clientMap.get(clientId);
    }

    public NFS4Client getClient(StateId stateId) {
        long clientId = getLong(stateId.other, 0);
        NFS4Client client = clientMap.get(clientId);
        if (client == null) {
            throw new NFSException(NFS4ERR_BAD_STATEID, "getClient : client not exist, bad stateId");
        }
        return client;
    }

    public NFS4Client getClient(byte[] sessionId) {
        long clientId = getLong(sessionId, 0);
        NFS4Client client = clientMap.get(clientId);
        if (client == null) {
            throw new NFSException(NFS4ERR_BADSESSION, "getClient : client not exist , bad session");
        }
        return client;
    }

    public NFS4Client getClient0(byte[] sessionId) {
        long clientId = getLong(sessionId, 0);
        return clientMap.get(clientId);
    }

    public NFS4Client clientByOwner(byte[] ownerId, SocketAddress address, SocketAddress localAddress) {
        List<NFS4Client> clients = new ArrayList<>(clientMap.values());
        return clients.stream()
                .filter(client -> Arrays.equals(client.getOwnerId(), ownerId)
                        && address.host().equals(client.getClientAddress())
                        && localAddress.host().equals(client.getLocalAddress()))
                .findFirst()
                .orElse(null);
    }

    public void updateClientLeaseTime(StateId stateId) {
        NFS4Client client = getClient(stateId);
        NFS4State state = client.state(stateId);
        if (!state.getConfirmed()) {
            throw new NFSException(NFS4ERR_BAD_STATEID, "bad stateId");
        }
        StateId.checkStateId(state.stateId(), stateId);
        client.updateLeaseTime();
    }

    public List<NFS4Client> getClients() {
        return new ArrayList<>(clientMap.values());
    }

    public NFS4Client createClient(SocketAddress clientAddress, SocketAddress localAddress, int minorVersion,
                                   byte[] ownerID, byte[] verifier, Auth auth, NFSHandler nfsHandler) {
        NFS4Client client = new NFS4Client(this, createClientId(), minorVersion, clientAddress,
                localAddress, ownerID, verifier, auth, TimeUnit.SECONDS.toMillis(leaseTime), nfsHandler);
        addClient(client);
        return client;
    }

    public boolean isGracePeriod() {
//        return clientOps.waitingForReclaim();
        return false;
    }


    public void reclaimComplete(byte[] owner) {
//        clientOps.reclaimClient(owner);
    }

    public void wantReclaim(byte[] owner) {
//        clientOps.wantReclaim(owner);
    }

    private void clearClients() {
        clientMap.values()
                .forEach(c -> {
                    c.tryDispose().subscribe();
                    clientMap.remove(c.getClientId());
                });
    }

    public int getInstanceId() {
        return instanceId;
    }


    public int getInstanceId(StateId stateid) {
        long clientId = getLong(stateid.other, 0);
        return (int) (clientId >> 16) & 0xFFFF;
    }

    public long createClientId() {
        long now = (System.currentTimeMillis() / 1000);
        return (now << 32) | ((long) instanceId << 16) | (clientId.incrementAndGet() & 0x0000FFFF);
    }

    public StateId createStateId(long clientId, int count) {
        byte[] other = new byte[12];
        putLong(other, 0, clientId);
        putInt(other, 8, count);
        return new StateId(0, other);
    }

    public byte[] createSessionId(long clientId, int sequence) {
        byte[] sessionId = new byte[16];
        putLong(sessionId, 0, clientId);
        putInt(sessionId, 12, sequence);
        return sessionId;
    }

    public int getLeaseTime() {
        return leaseTime;
    }

}
