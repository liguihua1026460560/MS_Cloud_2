package com.macrosan.message.jsonmsg;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.vertx.core.json.Json;

import java.io.IOException;

public class CustomJsonDateDeserializer extends JsonDeserializer {

    @Override
    public Object deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        String text = jsonParser.getText();
        if (text.contains("[")) {
            return Json.decodeValue(text, String[].class);
        }else {
            return text;
        }
    }
}
