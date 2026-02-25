package com.macrosan.rsocket.filesystem;

import com.macrosan.database.mongodb.MsMongoClient;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.httpserver.ServerConfig;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.macrosan.constants.SysConstants.REDIS_MAPINFO_INDEX;
import static com.macrosan.constants.SysConstants.VNODE_TAKE_OVER;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.include;

/**
 * PartFileMapping
 * <p>
 * 分段文件的映射，用于对应文件位置和段数
 *
 * @author liyixin
 * @date 2019/3/11
 */
@Log4j2
class PartFileMapping {

    private static final String PART_UPLOAD_MONGO_NAME = "PART_UPLOAD";

    private int index = 0;

    private Map<Long, String> startPosMapping = new HashMap<>(16);

    private long[] startPosArray;

    private final String masterVip1 = ServerConfig.getInstance().getMasterVip1();

    private final String masterVip2 = ServerConfig.getInstance().getMasterVip2();

    private static RedisConnPool pool = RedisConnPool.getInstance();

    PartFileMapping(String objId, String vnode) {
        String takeOver = getTakeOver(vnode);

        MongoCollection<Document> collection = MsMongoClient
                .getInstance("127.0.0.1", "127.0.0.1", takeOver)
                .getCollection(PART_UPLOAD_MONGO_NAME, "objectId");

        Bson eq = eq("key", objId);
        startPosArray = new long[(int) collection.countDocuments(eq)];

        MongoCursor<Document> cursor = collection.find(eq)
                .projection(include("partName", "index")).iterator();

        int count = 0;
        while (cursor.hasNext()) {
            Document doc = cursor.next();
            startPosMapping.put(doc.getLong("index"), doc.getString("partName"));
            startPosArray[count++] = doc.getLong("index");
        }
        Arrays.sort(startPosArray);
    }

    /**
     * 找到 pos 所在的文件，并且将index指向这个文件
     *
     * @param pos 要定位的位置
     */
    private void findNearest(long pos) {
        if (startPosArray.length == 1 || pos < startPosArray[1]) {
            index = 0;
        }
        int left = 0;
        int rigth = startPosArray.length - 1;
        while (rigth - left > 1) {
            if (startPosArray[(left + rigth) / 2] < pos) {
                left = (left + rigth) / 2;
            } else if (startPosArray[(left + rigth) / 2] > pos) {
                rigth = (left + rigth) / 2;
            } else {
                index = (left + rigth) / 2;
                return;
            }
        }
        index = left;
    }

    /**
     * 找到目标位置相较于当前index所指向文件的相对位置
     * <p>
     * 注：该方法会将index指向距离 pos 最近的文件
     *
     * @param pos 要定位的位置
     * @return 相对位置
     */
    long getRelativePos(long pos) {
        findNearest(pos);
        return pos - startPosArray[index];
    }

    /**
     * 获取当前index所指向的文件地址
     *
     * @return 当前index所指向的文件地址
     */
    String getCurrentFile() {
        return startPosMapping.get(startPosArray[index]);
    }

    /**
     * 指向下一个文件，并且返回文件地址
     *
     * @return 下一个文件地址
     */
    String getNextFile() {
        index = index + 1;
        try {
            return startPosMapping.get(startPosArray[index]);
        } catch (ArrayIndexOutOfBoundsException e) {
            index = index - 1;
            return "";
        }
    }

    /**
     * @param vnode vnode编号
     * @return 节点接管状态
     * @description 从redis里面查询节点接管状态 注：由于缺少库，这里的实现效率非常低下
     */
    private String getTakeOver(String vnode) {
        try {
            return pool.getCommand(REDIS_MAPINFO_INDEX).hget(vnode, VNODE_TAKE_OVER);
        } catch (Exception e) {
            log.error(masterVip1 + " fail to connect redis, will use " + masterVip2 + " to connect redis! fail msg: " + e.getMessage());
            try {
                return pool.getCommand(REDIS_MAPINFO_INDEX).hget(vnode, VNODE_TAKE_OVER);
            } catch (Exception e1) {
                log.error(masterVip2 + " also fail to connect redis! Create Mapping fail");
                throw e1;
            }
        }
    }
}
