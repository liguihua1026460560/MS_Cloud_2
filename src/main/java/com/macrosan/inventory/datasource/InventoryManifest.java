package com.macrosan.inventory.datasource;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.macrosan.inventory.datasource.scanner.Scanner;
import com.macrosan.message.xmlmsg.inventory.Schedule;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.serialize.JsonUtils;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.concurrent.Queues;

import java.util.ArrayList;
import java.util.List;

@Log4j2
@Data
public class InventoryManifest implements DataSource {

    public InventoryManifest(String fileName, String sourceBucket, String destinationBucket, String version, String creationTimestamp, String fileFormat, String[] fileSchema) {
        this.fileName = fileName;
        this.sourceBucket = sourceBucket;
        this.destinationBucket = destinationBucket;
        this.version = version;
        this.creationTimestamp = creationTimestamp;
        this.fileFormat = fileFormat;
        this.fileSchema = fileSchema;
        this.inventoryType = "Total";
    }

    public InventoryManifest(String fileName, String sourceBucket, String destinationBucket, String version, String creationTimestamp, String fileFormat, String[] fileSchema, String inventoryType){
      this.fileName = fileName;
      this.sourceBucket = sourceBucket;
      this.destinationBucket = destinationBucket;
      this.version = version;
      this.creationTimestamp = creationTimestamp;
      this.fileFormat = fileFormat;
      this.fileSchema = fileSchema;
      this.inventoryType = inventoryType;
    }

    @Data
    public static class InventoryFile {
        private final String key;
        private final long size;

        @JSONField(name = "MD5checksum")
        private final String MD5checksum;

        public InventoryFile(String key, long size, String md5checksum) {
            this.key = key;
            this.size = size;
            MD5checksum = md5checksum;
        }
    }

    private final String fileName;
    private final String sourceBucket;
    private final String destinationBucket;
    private final String version;
    private final String creationTimestamp; // 任务开始时间
    private final String fileFormat;
    private final String[] fileSchema;
    private final List<InventoryFile> files = new ArrayList<>();
    private final String inventoryType;

    private transient final UnicastProcessor<Tuple2<byte[], byte[]>> bytesFlux = UnicastProcessor.create(Queues.<Tuple2<byte[], byte[]>>unboundedMultiproducer().get());

    private final JSONObject jsonObject = new JSONObject(true);

    public JSONObject buildManifest() {
        JSONObject manifest = new JSONObject();
        manifest.put("fileName", fileName);
        manifest.put("sourceBucket", sourceBucket);
        manifest.put("destinationBucket", destinationBucket);
        manifest.put("version", version);
        manifest.put("creationTimestamp", creationTimestamp);
        manifest.put("fileFormat", fileFormat);
        manifest.put("fileSchema", fileSchema);
        manifest.put("inventoryType", inventoryType);
        return manifest;
    }

    @Override
    public Mono<Boolean> start() {
        jsonObject.put("sourceBucket", sourceBucket);
        jsonObject.put("destinationBucket", destinationBucket);
        jsonObject.put("version", version);
        jsonObject.put("creationTimestamp", creationTimestamp);
        jsonObject.put("fileFormat", fileFormat);
        StringBuilder fileSchemaString = new StringBuilder();
        for (String schema : fileSchema) {
            fileSchemaString.append(schema).append(",");
        }
        jsonObject.put("fileSchema", fileSchemaString.substring(0, fileSchemaString.length() - 1));
        jsonObject.put("files", files);
        jsonObject.put("inventoryType", inventoryType);
        return Mono.just(true);
    }



    @Override
    public void fetch(long n) {
        String string = jsonObject.toString();
        bytesFlux.onNext(new Tuple2<>(null, JsonUtils.beautify(string).getBytes()));
        bytesFlux.onComplete();
    }

    @Override
    public Flux<Tuple2<byte[], byte[]>> data() {
        return bytesFlux;
    }

    @Override
    public void complete() {
        bytesFlux.onComplete();
    }

    @Override
    public void release() {

    }

    @Override
    public void reset() {

    }

    @Override
    public boolean hasRemaining() {
        return false;
    }

    @Override
    public byte[] cursor(byte[] newCursor) {
        return null;
    }

    @Override
    public Scanner scanner() {
        return null;
    }

    @Override
    public Flux<Long> keepalive() {
        return null;
    }
}
