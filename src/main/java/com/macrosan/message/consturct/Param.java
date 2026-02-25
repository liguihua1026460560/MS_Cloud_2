package com.macrosan.message.consturct;

import com.macrosan.httpserver.MsHttpRequest;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Param
 * <p>
 * 用于标记{@link MsHttpRequest}中哪些成员会被打包往同步流传
 *
 * @author liyixin
 * @date 2018/10/28
 */
@Target({ElementType.FIELD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Param {

    /**
     * @return 成员变量在map中的key值
     */
    String key() default "";
}
