package com.macrosan.inventory;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.inventory.datasource.DataSource;
import com.macrosan.inventory.datasource.InventoryDefaultDataSource;
import com.macrosan.inventory.datasource.InventoryManifest;
import com.macrosan.inventory.datasource.scanner.*;
import com.macrosan.inventory.translator.AbstractStreamTranslator;
import com.macrosan.inventory.translator.CsvStreamTranslator;
import com.macrosan.inventory.transmitter.InventoryTransmitter;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.xmlmsg.inventory.InventoryConfiguration;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static com.macrosan.constants.SysConstants.REDIS_TASKINFO_INDEX;

@Log4j2
public final class InventoryTaskFactory {

    private final InventoryConfiguration inventoryConfiguration;
    private DataSource dataSource;
    private InventoryTransmitter transmitter;
    private final String sourceBucket;
    private final String disk;
    private StoragePool metaPool;

    private InventoryTaskFactory(String sourceBucket, String disk, InventoryConfiguration inventoryConfiguration) {
        this.inventoryConfiguration = inventoryConfiguration;
        this.sourceBucket = sourceBucket;
        this.disk = disk;
        metaPool = StoragePoolFactory.getMetaStoragePool(sourceBucket);
    }

    /**
     * 构建Inventory任务实例
     * @param sourceBucket 源桶
     * @param disk 源桶所在磁盘
     * @param inventoryConfiguration Inventory配置信息
     * @return Inventory任务实例
     */
    public static InventoryTask buildTask(String sourceBucket, String disk, InventoryConfiguration inventoryConfiguration) {
        return InventoryTaskFactory
                .factory(sourceBucket, disk, inventoryConfiguration)
                .datasource()
                .transmitter()
                .buildTask();
    }

    /**
     * 初始化InventoryTaskFactory
     * @param inventoryConfiguration Inventory配置
     * @return 实例
     */
    private static InventoryTaskFactory factory(String sourceBucket, String disk, InventoryConfiguration inventoryConfiguration) {
        return new InventoryTaskFactory(sourceBucket, disk, inventoryConfiguration);
    }

    /**
     * 添加数据源
     */
    private InventoryTaskFactory datasource() {
        Objects.requireNonNull(inventoryConfiguration);
        String vnode = metaPool.getBucketVnodeId(sourceBucket);
        String prefix = inventoryConfiguration.getFilter() != null && StringUtils.isNotEmpty(inventoryConfiguration.getFilter().getPrefix()) ? inventoryConfiguration.getFilter().getPrefix() : "";
        if (prefix.startsWith(File.separator) && prefix.length() > 1) {
            prefix = prefix.substring(1);
        }

        String includedObjectVersions = inventoryConfiguration.getIncludedObjectVersions();
        List<String> fields = inventoryConfiguration.getOptionalFields().getFields();
        String inventoryType = inventoryConfiguration.getInventoryType();

        if ("All".equals(includedObjectVersions)) {
            fields.add("VersionId");
            fields.add("IsDeleteMarker");
            fields.add("IsLatest");
        }

        String[] fieldArray = fields.toArray(new String[0]);
        AbstractStreamTranslator<MetaData> translator = null;
        InventoryFormat format = InventoryFormat.valueOf(inventoryConfiguration.getDestination().getS3BucketDestination().getFormat());
        switch (format) {
            case CSV:
                StringBuilder title = new StringBuilder();
                for (String field : fieldArray) {
                    title.append(field);
                    title.append(",");
                }

                if (title.length() != 0) {
                    title = new StringBuilder(title.substring(0, title.lastIndexOf(",")));
                    title.append("\r\n");
                }
                translator = new CsvStreamTranslator(fieldArray, title.toString().getBytes());
                break;
            case ORC:
            case Parquet:
            default:
                log.info("the file format is not supported!");
        }

        Scanner<Tuple2<byte[], byte[]>> scanner;
        if ("All".equals(includedObjectVersions)) {
            if("Incremental".equals(inventoryType)){
                String stamp = RedisConnPool.getInstance().getShortMasterCommand(REDIS_TASKINFO_INDEX).hget(sourceBucket + "_incrementalStamp", inventoryConfiguration.getId());
                scanner = new InventoryListStampScanner(sourceBucket, stamp, false);
            }else {

                scanner = new InventoryListAllScanner(sourceBucket, prefix);
            }
        } else {
            if("Incremental".equals(inventoryType)){
                /** 从redis 中获取起始时间戳*/
                String stamp = RedisConnPool.getInstance().getShortMasterCommand(REDIS_TASKINFO_INDEX).hget(sourceBucket + "_incrementalStamp", inventoryConfiguration.getId());
                scanner = new InventoryListStampScanner(sourceBucket, stamp, true);
            }else{
                scanner = new InventoryListCurrentScanner(sourceBucket, prefix);
            }
        }
        this.dataSource = new InventoryDefaultDataSource(scanner);
        ((InventoryDefaultDataSource) dataSource).setTranslator(translator);
        return this;
    }

    /**
     * 设置传输器
     */
    private InventoryTaskFactory transmitter() {
        Objects.requireNonNull(dataSource);
        transmitter = new InventoryTransmitter(this.dataSource);
        return this;
    }

    private InventoryTask buildTask() {
        try {
            Objects.requireNonNull(inventoryConfiguration);
            Objects.requireNonNull(dataSource);
            Objects.requireNonNull(transmitter);
            String targetBucket = inventoryConfiguration.getDestination().getS3BucketDestination().getBucket();
            String prefix = "";
            if (inventoryConfiguration.getDestination() != null
                    && inventoryConfiguration.getDestination().getS3BucketDestination()!= null
                    && StringUtils.isNotEmpty(inventoryConfiguration.getDestination().getS3BucketDestination().getPrefix())) {
                prefix = inventoryConfiguration.getDestination().getS3BucketDestination().getPrefix() + File.separator;
                if (prefix.startsWith(File.separator)) {
                    prefix = prefix.substring(1);
                }
            }
            if (StringUtils.isEmpty(prefix)) {
                prefix = "BucketInventory" + File.separator;
            }
            long taskCreationTimestamp = System.currentTimeMillis();
            Date date = new Date(taskCreationTimestamp);
            SimpleDateFormat dateFormatVersion = new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat dateFormatPath = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            String version = dateFormatVersion.format(date);
            InventoryFormat format = InventoryFormat.valueOf(inventoryConfiguration.getDestination().getS3BucketDestination().getFormat());
            List<String> fields = inventoryConfiguration.getOptionalFields().getFields();
            String[] schema = fields.toArray(new String[0]);
            String suffix = "." + format.name().toLowerCase();
            String uuid = UUID.randomUUID().toString();
            //是否是增量桶
            boolean isIncremental = "Incremental".equals(inventoryConfiguration.getInventoryType());

            String targetObject =  prefix + sourceBucket + File.separator + inventoryConfiguration.getId() + File.separator + dateFormatPath.format(date) + File.separator + "files" + File.separator + uuid + suffix;
            String manifestFileName = prefix + sourceBucket + File.separator + inventoryConfiguration.getId() + File.separator + dateFormatPath.format(date) + File.separator + "files" + File.separator + "manifest.json";

            if(isIncremental){
              targetObject = prefix + sourceBucket + File.separator + inventoryConfiguration.getId() + File.separator +
                      dateFormatPath.format(date) + File.separator + "files" + File.separator + "incremental" + File.separator + uuid + suffix;

              manifestFileName = prefix + sourceBucket + File.separator + inventoryConfiguration.getId() + File.separator + dateFormatPath.format(date) +
                      File.separator + "files" + File.separator + "incremental" + File.separator + "manifest.json";

            }

            InventoryManifest manifest = new InventoryManifest(manifestFileName, sourceBucket, targetBucket, version, String.valueOf(taskCreationTimestamp), format.name(), schema);
            if(isIncremental){
              manifest = new InventoryManifest(manifestFileName, sourceBucket, targetBucket, version, String.valueOf(taskCreationTimestamp), format.name(), schema, inventoryConfiguration.getInventoryType());
            }
            String inventoryId = inventoryConfiguration.getId();
            return new InventoryTask(dataSource, manifest, transmitter, inventoryId, sourceBucket, targetBucket, targetObject);
        } catch (Exception e) {
            log.error("build inventory task error!", e);
            return null;
        }
    }


}
