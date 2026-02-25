package com.macrosan.message.socketmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.dslplatform.json.JsonReader;
import com.dslplatform.json.JsonWriter;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.Map;

/**
 * ListMapResMsg
 *
 * @author shilinyong
 * @date 2019/7/31
 */
@EqualsAndHashCode(callSuper = true)
@CompiledJson
@Data
public class ListMapResMsg extends BaseResMsg {

    @JsonAttribute(name = "res_data", converter = ListStringOrLongConverter.class)
    public List<Map<String, String>> data;

    public static class ListStringOrLongConverter {
        public static JsonReader.ReadObject<List<Map<String, String>>> JSON_READER =
                reader -> reader.readCollection(MapResMsg.StringOrLongConverter.JSON_READER);

        public static JsonWriter.WriteObject<List<Map<String, String>>> JSON_WRITER = (writer, value) -> {
            writer.serialize(value, MapResMsg.StringOrLongConverter.JSON_WRITER);
        };
    }
}
