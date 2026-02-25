package com.macrosan.utils.property;

import com.macrosan.utils.functional.ImmutableTuple;
import io.netty.util.collection.IntObjectHashMap;
import lombok.Cleanup;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.io.*;
import java.util.Optional;
import java.util.Properties;

import static com.macrosan.constants.SysConstants.CONF_PATH_LIST;

/**
 * PropertyReader
 * 读配置文件的工具类
 *
 * @author liyixin
 * @date 2018/11/30
 */
public class PropertyReader {

    private static Logger logger = LogManager.getLogger(PropertyReader.class.getName());

    private static final String DEFAULT = "";

    private Properties properties;

    private String path;

    /**
     * 标示配置文件在工程内还是在工程外 true：在外面    false：在里面
     */
    private boolean flag;

    public PropertyReader(String file) {
        this.properties = new Properties();
        try {
            @Cleanup InputStream inputStream = null;
            if (CONF_PATH_LIST.contains(file)) {
                path = file;
                flag = true;
                inputStream = new BufferedInputStream(new FileInputStream(path));
            } else {
                path = PropertyReader.class.getResource(file).getPath();
                flag = false;
                inputStream = PropertyReader.class.getResourceAsStream(file);
            }
            properties.load(inputStream);
        } catch (Exception e) {
            logger.error("File not found :", e);
        }
    }

    /**
     * 读取配置文件中一个指定的value
     *
     * @param key 要读取的key
     * @return key对应的value
     */
    public String getPropertyAsString(String key) {
        return Optional.ofNullable(properties.getProperty(key)).orElseGet(() -> {
            logger.info("Property : " + key + " is null");
            return DEFAULT;
        });
    }

    public String getPropertyAsString(String key, String defaultValue) {
        return Optional.ofNullable(properties.getProperty(key)).orElseGet(() -> {
            logger.info("Property : " + key + " is null");
            return defaultValue;
        });
    }

    /**
     * 返回整个配置文件
     *
     * @return 所有值非空的键值对
     */
    public UnifiedMap<String, String> getPropertyAsMap() {
        return properties.stringPropertyNames().stream()
                .map(s -> new ImmutableTuple<>(s, properties.getProperty(s)))
                .filter(tuple -> !StringUtils.isBlank(tuple.var2))
                .collect(UnifiedMap::new, (map, tuple) -> map.put(tuple.var1, tuple.var2), UnifiedMap::putAll);

    }

    /**
     * 返回整个配置文件，value为list形式，以 {@code /}分割
     *
     * @return 所有非空键值对
     */
    public IntObjectHashMap<String[]> getPropertyAsMultiMap() {
        return properties.stringPropertyNames().stream()
                .map(s -> new ImmutableTuple<>(Integer.valueOf(s), properties.getProperty(s).split("/")))
                .collect(IntObjectHashMap::new, (map, tuple) -> map.put(tuple.var1, tuple.var2), IntObjectHashMap::putAll);
    }

    /**
     * 往配置文件中添加或更新键值对
     *
     * @param key   要添加或更新的key
     * @param value 要添加或更新的key对应的value
     */
    public void setProperty(String key, String value) {
        /* 暂不支持修改JAR包中的配置文件，等有这个需求再说 */
        if (!flag) {
            return;
        }
        properties.setProperty(key, value);
        try {
            Writer writer = new BufferedWriter(new FileWriter(path));
            properties.store(writer, "");
            writer.close();
        } catch (IOException e) {
            logger.error("write config fail :", e);
        }
    }

    public void setProperties(String key, String value) {
        properties.setProperty(key, value);
    }

    public void restore() {
        try {
            Writer writer = new BufferedWriter(new FileWriter(path));
            properties.store(writer, "");
            writer.close();
        } catch (IOException e) {
            logger.error("write config fail :", e);
        }
    }
}
