package com.macrosan.database.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * MongodbConstants
 *
 * @author liyixin
 * @date 2019/2/26
 */
public class MongodbConstants {

    public MongodbConstants() {
    }

    static final String TAKE_OVER = "1";

    static final int TAKE_OVER_PORT = 27018;

    static final int DEFAULT_PORT = 27017;

    static final String ADDRESS_HEAD = "mongodb://";

    //    static final String OPTION = "/?connectTimeoutMS=300&socketTimeoutMS=300&serverSelectionTimeoutMS=300";
    static final String OPTION = "";

    static final Class<?>[] CLS = new Class<?>[]{MongoCollection.class};

    static final Class<?>[] MDBCLS = new Class<?>[]{MongoDatabase.class};
}
