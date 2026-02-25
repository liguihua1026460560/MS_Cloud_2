package com.macrosan.inventory.translator;

public abstract class AbstractStreamTranslator<T> {

    /**
     * 需要展示的对象的字段
     */
    protected String[] optionFields;

    public byte[] title;

    protected AbstractStreamTranslator(String[] optionFields, byte[] title) {
        this.optionFields = optionFields;
        this.title = title;
    }

    /**
     * 将元数据格式转换成需要的数据流形式
     * @param t 元数据
     * @return 数据流
     * @throws Exception
     */
    public abstract byte[] translate(T t) throws Exception;
}
