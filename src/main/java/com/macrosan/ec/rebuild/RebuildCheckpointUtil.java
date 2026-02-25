package com.macrosan.ec.rebuild;

import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.ec.migrate.SstVnodeDataScannerUtils;
import com.macrosan.storage.NodeCache;
import com.macrosan.utils.functional.Tuple3;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author zhaoyang
 * @date 2024/11/12
 **/
@Log4j2
public class RebuildCheckpointUtil {

    public static final String REBUILD_CHECK_POINT_SUFFIX = "rebuild_check_point";

    public static String getCheckPointLun(String lun) {
        if (!lun.contains(File.separator)) {
            return lun + File.separator + "rocks_db" + File.separator + REBUILD_CHECK_POINT_SUFFIX;
        }
        return lun + File.separator + REBUILD_CHECK_POINT_SUFFIX;
    }

    public static void createCheckPoint(String disk) throws RocksDBException {
        String cpPath = File.separator + getCheckPointLun(disk);
        if (Files.exists(Paths.get(cpPath))) {
            SstVnodeDataScannerUtils.removeCheckPoint(cpPath);
        }
        MSRocksDB db = MSRocksDB.getRocksDB(disk);
        if (db == null) {
            return;
        }
        Checkpoint checkpoint = Checkpoint.create(db.getRocksDB());
        checkpoint.createCheckpoint(cpPath);
        log.info("createCheckPoint,path:{}", cpPath);
    }

    public static void closeCheckPoint(String lun) {
        MSRocksDB.remove(getCheckPointLun(lun));
        SstVnodeDataScannerUtils.removeCheckPoint("/" + getCheckPointLun(lun));
    }

    /**
     * 将tuple3格式的节点信息转为lun
     * tuple3(ip,lun,vnode)---> uuid@lun
     *
     * @param nodeInfo 节点信息
     * @return lun
     */
    public static String nodeListMapToLun(Tuple3<String, String, String> nodeInfo) {
        return NodeCache.mapIPToNode(nodeInfo.var1) + "@" + nodeInfo.var2;
    }


    /**
     * 检查目录是否存在
     *
     * @param dirPath 目录路径
     * @return 目录是否存在
     */
    public static boolean isDirectoryExists(String dirPath) {
        try {
            if (StringUtils.isBlank(dirPath)) {
                return true;
            }
            if (!dirPath.startsWith("/")) {
                dirPath = "/" + dirPath;
            }
            Path path = Paths.get(dirPath);
            return !Files.exists(path) || !Files.isDirectory(path);
        } catch (Exception e) {
            return true;
        }
    }
}
