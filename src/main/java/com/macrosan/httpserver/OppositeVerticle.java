package com.macrosan.httpserver;

import io.reactivex.disposables.Disposable;
import io.vertx.core.Future;

import java.util.HashSet;
import java.util.Set;

/**
 * OppositeVerticle
 *
 * @author liyixin
 * @date 2019/10/22
 */
public class OppositeVerticle extends RestfulVerticle {

    private static String bindIp1;

    private static String bindIp2;

    private static String bindIpV61;

    private static String bindIpV62;

    private static Set<String> deployIds = new HashSet<>();

    public static void init(String... ips) {
        OppositeVerticle.bindIp1 = ips[0];
        OppositeVerticle.bindIp2 = ips[1];
        if (ips.length>2){
            OppositeVerticle.bindIpV61 = ips[2];
            OppositeVerticle.bindIpV62 = ips[3];
        }
    }

    public static void unDeploy() {
        deployIds.forEach(s -> ServerConfig.getInstance().getVertx().undeploy(s));
        deployIds.clear();
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        super.stop(stopFuture);
        getStatusList().forEach(Disposable::dispose);
    }

    @Override
    public void start() {
        deployIds.add(vertx.getOrCreateContext().deploymentID());
        start0(bindIp1, bindIp2, bindIpV61, bindIpV62);
    }
}
