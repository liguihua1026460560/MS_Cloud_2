package com.macrosan.message.socketmsg;

import com.dslplatform.json.*;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.IOException;
import java.util.Map;

/**
 * MapResMsg
 *
 * @author liyixin
 * @date 2019/1/10
 */
@EqualsAndHashCode(callSuper = true)
@CompiledJson
@Data
public class MapResMsg extends BaseResMsg {

    @JsonAttribute(name = "res_data", converter = StringOrLongConverter.class)
    public Map<String, String> data;

    public static class StringOrLongConverter {
        public static JsonReader.ReadObject<Map<String, String>> JSON_READER = reader -> {
            return reader.readMap(StringConverter.READER, new JsonReader.ReadObject<String>() {
                @Nullable
                @Override
                public String read(JsonReader reader) throws IOException {
                    if (reader.wasNull()) return null;
                    try {
                        return reader.readString();
                    } catch (ParsingException e) {
                        return NumberConverter.LONG_READER.read(reader) + "";
                    }
                }
            });
        };

        public static JsonWriter.WriteObject<Map<String, String>> JSON_WRITER = (writer, value) -> {
            writer.serialize(value, StringConverter.WRITER, StringConverter.WRITER);
        };
    }
}
