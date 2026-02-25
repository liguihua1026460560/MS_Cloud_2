package com.macrosan.utils.serialize;

import com.dslplatform.json.DslJson;
import com.dslplatform.json.JsonWriter;
import com.dslplatform.json.runtime.Settings;
import io.vertx.core.buffer.Buffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.io.IOException;

import static com.macrosan.constants.ServerConstants.PROC_NUM;

/**
 * JsonUtils
 * 提供JSON 序列化和反序列化等功能,由dsl-json库实现
 *
 * @author liyixin
 * @date 2018/11/4
 */
public class JsonUtils {
    private static Logger logger = LogManager.getLogger(JsonUtils.class.getName());

    private static final DslJson<Object> JSON = new DslJson<>(Settings.withRuntime().allowArrayFormat(true).includeServiceLoader());

    private static LongObjectHashMap<JsonWriter> writerMap = new LongObjectHashMap<>((int) (PROC_NUM * 2 / 0.75) + 1);

    private JsonUtils() {
    }

    private static JsonWriter getWriter() {
        long key = Thread.currentThread().getId();
        if (!writerMap.containsKey(key)) {
            writerMap.put(key, JSON.newWriter());
        }
        return writerMap.get(key);
    }

    public static void toBuffer(Object msg, Class<?> cls, Buffer buffer) {
        JsonWriter writer = getWriter();
        JSON.serialize(writer, cls, msg);
        buffer.appendInt(writer.size()).appendBytes(writer.toByteArray());
        writer.reset();
    }

    public static byte[] toByteArray(Object msg) {
        JsonWriter writer = getWriter();
        try {
            JSON.serialize(writer, msg);
            byte[] bytes = writer.getByteBuffer();
            writer.reset();
            return bytes;
        } catch (IOException e) {
            logger.error("serialize fail, e :", e);
        }
        return new byte[0];
    }

    public static String toString(Object msg) {
        JsonWriter writer = getWriter();
        try {
            JSON.serialize(writer, msg);
            String str = writer.toString();
            writer.reset();
            return str;
        } catch (IOException e) {
            logger.error("serialize fail, e :", e);
        }
        return "";
    }

    public static String toString(Object msg, Class<?> cls) {
        JsonWriter writer = getWriter();
        JSON.serialize(writer, cls, msg);
        String str = writer.toString();
        writer.reset();
        return str;
    }

    public static <T> T toObject(Class<T> cls, byte[] bytes) {
        try {
            return JSON.deserialize(cls, bytes, bytes.length);
        } catch (IOException e) {
            logger.error("deserialize fail, e :", e);
        }
        return null;
    }

    /**
     * 单位缩进字符串。
     */
    private static String SPACE = "   ";


    /**
     * 返回格式化JSON字符串。
     * @param json 未格式化的JSON字符串。
     * @return 格式化的JSON字符串。
     *
     * eg:
     * {
     *    "inventoryConfiguration":{
     *       "destination":{
     *          "s3BucketDestination":{
     *             "bucketArn":"bucket-3",
     *             "format":"CSV",
     *             "prefix":""
     *          }
     *       },
     *       "enabled":true,
     *       "id":"inventory-1",
     *       "includedObjectVersions":"All",
     *       "inventoryFilter":{
     *          "predicate":{
     *             "prefix":""
     *          }
     *       },
     *       "optionalFields":[
     *          "Bucket",
     *          "Key",
     *          "Size",
     *          "LastModifiedDate",
     *          "StorageClass",
     *          "ETag",
     *          "IsMultipartUploaded",
     *          "ObjectLockRetainUntilDate",
     *          "ObjectLockEnabled"
     *       ],
     *       "schedule":{
     *          "frequency":"Daily"
     *       }
     *    }
     * }
     *
     */
    public static String beautify(String json)
    {
        StringBuffer result = new StringBuffer();

        int length = json.length();
        int number = 0;
        char key = 0;
        //遍历输入字符串。
        for (int i = 0; i < length; i++)
        {
            //1、获取当前字符。
            key = json.charAt(i);

            //2、如果当前字符是前方括号、前花括号做如下处理：
            if((key == '[') || (key == '{') )
            {
                // 中括号的第一个花括号不换行
                if (key == '[' && ((i + 1) < length && (json.charAt(i + 1) == '{'))) {
                    result.append(key);
                    continue;
                }

                //（2）打印：当前字符。
                result.append(key);

                //（3）前方括号、前花括号，的后面必须换行。打印：换行。
                result.append('\n');

                //（4）每出现一次前方括号、前花括号；缩进次数增加一次。打印：新行缩进。
                number++;
                result.append(indent(number));

                //（5）进行下一次循环。
                continue;
            }

            //3、如果当前字符是后方括号、后花括号做如下处理：
            if((key == ']') || (key == '}') )
            {
                if (key == ']' && (i - 1) > 0 && json.charAt(i - 1) == '}') {
                    result.append(key);
                    continue;
                }
                //（1）后方括号、后花括号，的前面必须换行。打印：换行。
                result.append('\n');

                //（2）每出现一次后方括号、后花括号；缩进次数减少一次。打印：缩进。
                number--;
                result.append(indent(number));

                //（3）打印：当前字符。
                result.append(key);

                continue;
            }

            if((key == ',') && ((i + 1 < length) && json.charAt(i + 1) == '"' || ((i - 1 > 0) && (json.charAt(i - 1) == ']'))))
            {
                result.append(key);
                result.append('\n');
                result.append(indent(number));
                continue;
            }

            result.append(key);
        }

        return result.toString();
    }

    private static String indent(int number)
    {
        StringBuffer result = new StringBuffer();
        for(int i = 0; i < number; i++)
        {
            result.append(SPACE);
        }
        return result.toString();
    }
}
