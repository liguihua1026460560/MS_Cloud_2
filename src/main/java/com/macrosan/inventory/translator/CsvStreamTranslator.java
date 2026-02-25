package com.macrosan.inventory.translator;

import com.macrosan.inventory.InventoryField;
import com.macrosan.message.jsonmsg.MetaData;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;

public class CsvStreamTranslator extends AbstractStreamTranslator<MetaData> {
    public CsvStreamTranslator(String[] optionFields, byte[] title) {
        super(optionFields, title);
    }

    /**
     * 1.每条记录占一行。
     * 2.以逗号为分隔符。
     * 3.字段中包含有逗号，该字段必须用双引号括起来。
     * 4.字段中包含有换行符，该字段必须用双引号括起来。
     * 5.字段前后包含有空格，该字段必须用双引号括起来。
     * 6.字段中的双引号用两个双引号表示。
     * 7.字段中如果有双引号，该字段必须用双引号括起来。
     * 8.第一条记录，可以是字段名。
     */
    @Override
    public byte[] translate(MetaData metaData) throws Exception {
        InventoryField inventoryField = InventoryField.valueOf(metaData);
        StringBuilder line = new StringBuilder();
        //利用反射获取所有字段
        Field[] fields = inventoryField.getClass().getDeclaredFields();
        for(String property : optionFields){
            for(Field field : fields){
                //设置字段可见性
                field.setAccessible(true);
                if(property.equalsIgnoreCase(field.getName())){
                    if (field.get(inventoryField) != null) {
                        String string = field.get(inventoryField).toString();
                        // 内容中有英文逗号,或者换行符，或者引号
                        if (string.contains(",")|| string.contains("\n") || string.contains("\"")) {
                            // 内容中包含 " 使用 ""代替
                            if (string.contains("\"")) {
                                string = string.replaceAll("\"", "\"\"");
                            }
                            string = StringUtils.wrap(string, "\"");
                        }
                        line.append(string);
                        line.append(",");
                    } else {
                        line.append(",");
                    }
                }
            }
        }
        if (line.length() != 0) {
            line = new StringBuilder(line.substring(0, line.lastIndexOf(",")));
        }
        line.append("\r\n");
        return line.toString().getBytes();
    }
}
