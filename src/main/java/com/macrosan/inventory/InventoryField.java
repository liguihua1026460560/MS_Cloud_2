package com.macrosan.inventory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.utils.msutils.MsDateUtils;
import io.vertx.core.json.Json;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.macrosan.utils.worm.WormUtils.EXPIRATION;

/**
 * <p>
 *     用于封装清单列表中需要展示的对象元数据字段
 * </p>
 *
 * @author zhangjiliang
 */
@Data
public class InventoryField {

    private String bucket;
    private String key;
    private String size;
    private String lastModifiedDate;
    private String storageClass;
    private String eTag;
    private String isMultipartUploaded;
    private String objectLockRetainUntilDate;
    private String objectLockEnabled;

    private String versionId;
    private boolean isDeleteMarker;
    private Boolean isLatest;

    public static List<String> fields() {
        Class<InventoryField> inventoryFieldClass = InventoryField.class;
        Field[] fields = inventoryFieldClass.getDeclaredFields();
        return Arrays.stream(fields).map(Field::getName).map(fieldName -> {
            char[] chars = fieldName.toCharArray();
            chars[0] -= 32;
            return String.valueOf(chars);
        }).collect(Collectors.toList());
    }

    public static InventoryField valueOf(MetaData metaData) {
        InventoryField inventoryField = new InventoryField();
        inventoryField.setBucket(metaData.bucket);
        inventoryField.setKey(metaData.key);
        inventoryField.setSize(String.valueOf(metaData.endIndex - metaData.startIndex + 1));
        Map<String, String> sysMap = Json.decodeValue(metaData.sysMetaData, new TypeReference<Map<String, String>>() {});
        inventoryField.setIsMultipartUploaded("");
        inventoryField.setStorageClass("STANDARD");
        inventoryField.setLastModifiedDate(MsDateUtils.stampToDateFormat(metaData.stamp));
        String etag = sysMap.getOrDefault("ETag", "");
        String expiration = "";
        boolean notEmpty = StringUtils.isNotEmpty(sysMap.get(EXPIRATION));
        if (notEmpty) {
            String s = sysMap.get(EXPIRATION);
            expiration = MsDateUtils.stampToISO8601(s);
        }
        String enabled = "Disabled";
        if (!"".equals(expiration)) {
            enabled = "Enabled";
        }
        inventoryField.setETag(etag);
        inventoryField.setObjectLockRetainUntilDate(expiration);
        inventoryField.setObjectLockEnabled(enabled);
        inventoryField.setVersionId(metaData.versionId);
        inventoryField.setDeleteMarker(metaData.deleteMarker);
        return inventoryField;
    }
}
