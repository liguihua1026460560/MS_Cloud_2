package com.macrosan.ec;

/**
 * @author admin
 */

public enum GetMetaResEnum {

    /**
     * 查询不到
     */
    GET_NOT_FOUND("1"),

    /**
     * 查询失败
     */
    GET_ERROR("2");

    public String res;

    GetMetaResEnum(String res) {
        this.res = res;
    }
}
