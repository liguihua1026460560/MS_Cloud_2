package com.macrosan.filesystem.cifs.rpc.witness;

import com.macrosan.constants.ServerConstants;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.filesystem.cifs.rpc.witness.handler.WitnessHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.core.net.NetServerOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.net.NetServer;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;

import static com.macrosan.constants.SysConstants.REDIS_NODEINFO_INDEX;

@Log4j2
public class WITNESS {
    public static boolean cifsIPRegister = false;
    public static boolean witnessDebug = false;
    public static int WITNESS_MIN_PORT = 20000;
    public static int WITNESS_MAX_PORT = 20002;

    public static List<String> businessIps;
    static Vertx vertx;

    public static void start() {
        DeploymentOptions options = new DeploymentOptions()
                .setInstances(ServerConstants.PROC_NUM);
        vertx = Vertx.vertx(new VertxOptions()
                .setEventLoopPoolSize(Runtime.getRuntime().availableProcessors())
                .setPreferNativeTransport(true));

        vertx.rxDeployVerticle(WITNESS.WitnessVerticle.class.getName(), options).subscribe();
        log.info("start witness service in {} -> {}", WITNESS_MIN_PORT, WITNESS_MAX_PORT);

        businessIps = getBusinessIp();
    }

    public static class WitnessVerticle extends AbstractVerticle {
        @Override
        public void start() {
            NetServerOptions serverOptions = new NetServerOptions()
                    .setReuseAddress(true)
                    .setReusePort(true)
                    .setTcpKeepAlive(true);

            for (int port = WITNESS_MIN_PORT; port <= WITNESS_MAX_PORT; port++) {
                NetServer witness = WITNESS.vertx.createNetServer(serverOptions);
                int finalPort = port;
                witness.connectStream()
                        .toFlowable()
                        .subscribe(socket -> {
                            WitnessHandler handler = new WitnessHandler(socket);
                            socket.handler(b -> {
                                try {
                                    handler.handler(finalPort, b.getByteBuf());
                                } catch (Exception e) {
                                    log.error("", e);
                                    socket.close();
                                }
                            });

                            socket.resume();
                        });

                witness.listen(port, "0.0.0.0", res -> {
                    if (!res.succeeded()) {
                        log.error("Listen witnessPort failed, port : {}\ncause : {}", finalPort, res.cause());
                    }
                });
            }
        }
    }

    public static List<String> getBusinessIp() {
        List<String> keys = RedisConnPool.getInstance().getCommand(REDIS_NODEINFO_INDEX).keys("*");
        List<String> list = new ArrayList<>();
        for (String key : keys) {
            String business_eth1 = RedisConnPool.getInstance().getCommand(REDIS_NODEINFO_INDEX).hget(key, "business_eth1");
            if (business_eth1 == null) {
                log.error("node {} get business eth1 fail" , key);
            }
            list.add(business_eth1);
        }
        return list;
    }
}
