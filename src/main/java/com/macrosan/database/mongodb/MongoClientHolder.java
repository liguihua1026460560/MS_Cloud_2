package com.macrosan.database.mongodb;

import java.util.concurrent.ConcurrentHashMap;

/**
 * MongoClientHolder
 * <p>
 * 持有连接到不同Mongodb实例的MongoClient
 * <p>
 * TODO 可以考虑不使用并发容器
 *
 * @author liyixin
 * @date 2019/2/26
 */
class MongoClientHolder {

    private MongoClientHolder() {
    }

    private static ConcurrentHashMap<String, MsMongoClient> clientHolder = new ConcurrentHashMap<>(16);

    private static ConcurrentHashMap<String, MsMongoClient> takeOverClientHolder = new ConcurrentHashMap<>(16);

    static MsMongoClient get(String ip1, String ip2, String takeOver) {
        return MongodbConstants.TAKE_OVER.equals(takeOver) ? takeOverClientHolder.get(ip1 + "-" + ip2) : clientHolder.get(ip1 + "-" + ip2);
    }

    static void put(String ip1, String ip2, String takeOver, MsMongoClient instance) {
        if (MongodbConstants.TAKE_OVER.equals(takeOver)) {
            takeOverClientHolder.put(ip1 + "-" + ip2, instance);

        } else {
            clientHolder.put(ip1 + "-" + ip2, instance);
        }
    }

}
