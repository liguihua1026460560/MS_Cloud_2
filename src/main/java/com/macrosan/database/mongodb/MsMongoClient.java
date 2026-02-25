package com.macrosan.database.mongodb;


import com.mongodb.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.lang.reflect.Proxy;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author liyixin
 */
public class MsMongoClient {

    private Logger logger = LogManager.getLogger(MsMongoClient.class);

    private MongoClient internalClient;

    private String[] connectStr = new String[2];

    private int index = ThreadLocalRandom.current().nextInt(2);

    private long stamp = 0;

    private MsMongoClient(String ip1, String ip2, String takeOver) {
        int port = MongodbConstants.TAKE_OVER.equals(takeOver) ? MongodbConstants.TAKE_OVER_PORT
                : MongodbConstants.DEFAULT_PORT;
        connectStr[0] = MongodbConstants.ADDRESS_HEAD + ip1 + ":" + port + MongodbConstants.OPTION;
        connectStr[1] = MongodbConstants.ADDRESS_HEAD + ip2 + ":" + port + MongodbConstants.OPTION;
        internalClient = MongoClients.create(connectStr[index]);
    }

    public static MsMongoClient getInstance(String ip1, String ip2, String takeOver) {
        MsMongoClient client = MongoClientHolder.get(ip1, ip2, takeOver);
        if (client == null) {
            synchronized (MsMongoClient.class) {
                if ((client = MongoClientHolder.get(ip1, ip2, takeOver)) == null) {
                    client = new MsMongoClient(ip1, ip2, takeOver);
                    MongoClientHolder.put(ip1, ip2, takeOver, client);
                }
            }
        }
        return client;
    }

    @SuppressWarnings("unchecked")
    public MongoCollection<Document> getCollection(String dataBase, String collection) {
        Object target = internalClient.getDatabase(dataBase).getCollection(collection);
        return (MongoCollection<Document>) Proxy.newProxyInstance(MongoCollection.class.getClassLoader(),
                MongodbConstants.CLS, (proxy, method, args) -> {
                    try {
                        return method.invoke(target, args);
                    } catch (Exception e) {
                        failOver();
                        Object standby = internalClient.getDatabase(dataBase).getCollection(collection);
                        return method.invoke(standby, args);
                    }
                });
    }

    public MongoDatabase getDatabase(String dataBase) {
        Object target = internalClient.getDatabase(dataBase);
        return (MongoDatabase) Proxy.newProxyInstance(MongoDatabase.class.getClassLoader(), MongodbConstants.MDBCLS,
                (proxy, method, args) -> {
                    try {
                        return method.invoke(target, args);
                    } catch (Exception e) {
                        failOver();
                        Object standby = internalClient.getDatabase(dataBase);
                        return method.invoke(standby, args);
                    }
                });
    }

    public MongoIterable<String> getDbNames() {
        MongoIterable<String> dbNames;
        try {
            dbNames = internalClient.listDatabaseNames();
        } catch (Exception e) {
            failOver();
            dbNames = internalClient.listDatabaseNames();
        }
        return dbNames;
    }

    public synchronized void failOver() {
        if (System.currentTimeMillis() - stamp > 5000) {
            logger.info("address : " + connectStr[index] + " is unreachable, switch to : " + connectStr[1 - index]);
            internalClient.close();
            index = 1 - index;
            internalClient = MongoClients.create(connectStr[index]);
            stamp = System.currentTimeMillis();
        }
    }
}
