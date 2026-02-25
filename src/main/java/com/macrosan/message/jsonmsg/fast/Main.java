package com.macrosan.message.jsonmsg.fast;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.macrosan.message.jsonmsg.MetaData;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * 用于生成特定class的json方法
 */
public class Main {
    private static void genSerializer(Class clazz) throws Exception {
        String className = clazz.getSimpleName();

        System.out.println("import com.fasterxml.jackson.core.JsonGenerator;\n" +
                "import com.fasterxml.jackson.databind.JsonSerializer;\n" +
                "import com.fasterxml.jackson.databind.SerializerProvider;\n" +
                "import " + clazz.getName() + ";" +
                "import java.io.IOException;");

        System.out.println(String.format("public class %sSerializer extends JsonSerializer<%s> {", className, className));
        System.out.println("@Override");
        System.out.println(String.format("public void serialize(%s value, JsonGenerator gen, SerializerProvider serializers) throws IOException {", className));
        System.out.println("gen.writeStartObject(value);");
        Field[] field = clazz.getDeclaredFields();
        Object defaultValue = clazz.getConstructor().newInstance();

        for (Field f : field) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }

            f.setAccessible(true);
            String pro = f.getName();

            JsonProperty property = f.getAnnotation(JsonProperty.class);
            if (property != null) {
                pro = property.value();
            }

            Class type = f.getType();
            String name = f.getName();

            if (type == String.class) {
                String def = (String) f.get(defaultValue);
                String getter = "get" + name.substring(0, 1).toUpperCase() + name.substring(1);
                System.out.println(String.format("String %s = value.%s();", name, getter));
                if (def == null) {
                    System.out.println(String.format("if(%s != null) {", name));
                } else {
                    System.out.println(String.format("if(!\"%s\".equals(%s)) {", def, name));
                }
                System.out.println(String.format("gen.writeFieldName(\"%s\");", pro));
                System.out.println(String.format("MsStringJson.serialize0(%s, gen, serializers);", name));
                System.out.println("}");
            } else if (type == long.class) {
                long def = (Long) f.get(defaultValue);
                String getter = "get" + name.substring(0, 1).toUpperCase() + name.substring(1);
                System.out.println(String.format("long %s = value.%s();", name, getter));
                System.out.println(String.format("if(%s != %d) {", name, def));
                System.out.println(String.format("gen.writeFieldName(\"%s\");", pro));
                System.out.println(String.format("gen.writeNumber(%s);", name));
                System.out.println("}");
            } else if (type == boolean.class) {
                String getter = "is" + name.substring(0, 1).toUpperCase() + name.substring(1);
                System.out.println(String.format("boolean %s = value.%s();", name, getter));
                System.out.println(String.format("if(%s) {", name));
                System.out.println(String.format("gen.writeFieldName(\"%s\");", pro));
                System.out.println(String.format("gen.writeBoolean(%s);", name));
                System.out.println("}");
            } else if (type == long[].class) {
                String getter = "get" + name.substring(0, 1).toUpperCase() + name.substring(1);
                System.out.println(String.format("long[] %s = value.%s();", name, getter));
                System.out.println(String.format("if(%s != null) {", name));
                System.out.println(String.format("gen.writeFieldName(\"%s\");", pro));
                System.out.println("gen.writeStartArray();");
                System.out.println(String.format("for (int i = 0; i < %s.length; ++i) {", name));
                System.out.println(String.format("gen.writeNumber(%s[i]);", name));
                System.out.println("}");
                System.out.println("gen.writeEndArray();");
                System.out.println("}");
            } else {
                String getter = "get" + name.substring(0, 1).toUpperCase() + name.substring(1);
                System.out.println(String.format("Object %s = value.%s();", name, getter));
                System.out.println(String.format("if(%s != null) {", name));
                System.out.println(String.format("gen.writeFieldName(\"%s\");", pro));
                System.out.println(String.format("gen.writeObject(%s);", name));
                System.out.println("}");
            }
        }

        System.out.println("gen.writeEndObject();");
        System.out.println("}");
        System.out.println("}");
    }

    private static void genDeserializer(Class clazz) throws Exception {
        String className = clazz.getSimpleName();

        System.out.println("import com.fasterxml.jackson.core.JsonParser;\n" +
                "import com.fasterxml.jackson.databind.JsonDeserializer;\n" +
                "import com.fasterxml.jackson.databind.DeserializationContext;\n" +
                "import com.fasterxml.jackson.core.JsonProcessingException;\n" +
                "import com.fasterxml.jackson.core.JsonTokenId;" +
                "import " + clazz.getName() + ";" +
                "import com.fasterxml.jackson.databind.deser.std.PrimitiveArrayDeserializers;" +
                "import java.io.IOException;");

        System.out.println(String.format("public class %sDeserializer extends JsonDeserializer<%s> {", className, className));
        System.out.println("private static JsonDeserializer<long[]> LongArrayJsonDeserializer = (JsonDeserializer<long[]>) PrimitiveArrayDeserializers.forType(Long.TYPE);");

        System.out.println("@Override");
        System.out.println(String.format("public %s deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {", className));
        System.out.println("if (p instanceof MsReaderBasedJsonParser) {");
        Field[] field = clazz.getDeclaredFields();

        System.out.println("MsReaderBasedJsonParser parser = ((MsReaderBasedJsonParser) p);");
        String param = className.toLowerCase();
        System.out.println(String.format("%s %s = new %s();", className, param, className));
        System.out.println("p.nextToken();");

        System.out.println("if (p.hasTokenId(JsonTokenId.ID_FIELD_NAME)) {");
        System.out.println("String propName = p.getCurrentName();");

        System.out.println("do {");
        System.out.println("p.nextToken();");
        System.out.println("switch (propName) {");
        for (Field f : field) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            f.setAccessible(true);
            String pro = f.getName();

            JsonProperty property = f.getAnnotation(JsonProperty.class);
            if (property != null) {
                pro = property.value();
            }

            System.out.println(String.format("case \"%s\": ", pro));

            JsonAlias alias = f.getAnnotation(JsonAlias.class);
            if (alias != null) {
                for (String v : alias.value()) {
                    System.out.println(String.format("case \"%s\": ", v));
                }
            }

            Class type = f.getType();
            String name = f.getName();
            String setter = "set" + name.substring(0, 1).toUpperCase() + name.substring(1);

            if (type == String.class) {
                System.out.println(String.format("%s.%s(parser.getText0());", param, setter));
                System.out.println("break;");
            } else if (type == long.class) {
                System.out.println(String.format("%s.%s(parser.getLongValue());", param, setter));
                System.out.println("break;");
            } else if (type == boolean.class) {
                System.out.println(String.format("%s.%s(parser.getBooleanValue());", param, setter));
                System.out.println("break;");
            } else if (type == long[].class) {
                System.out.println(String.format("%s.%s(LongArrayJsonDeserializer.deserialize(p, ctxt));", param, setter));
                System.out.println("break;");
            } else {
                System.out.println(String.format("%s.%s(parser.getCurrentValue());", param, setter));
                System.out.println("break;");
            }
        }
        System.out.println("}");
        System.out.println(" } while ((propName = p.nextFieldName()) != null);");
        System.out.println("}");
        System.out.println(String.format("return %s;", param));


        System.out.println("} else {");
        System.out.println("throw new UnsupportedOperationException();");
        System.out.println("}");
        System.out.println("}");
        System.out.println("}");
    }

    public static void main(String[] args) throws Exception {
        genDeserializer(MetaData.class);
    }
}
