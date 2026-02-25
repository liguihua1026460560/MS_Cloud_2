package com.macrosan.ec.error;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface HandleErrorFunction {
    ErrorConstant.ECErrorType value();

    long timeout() default 33_000L;
}
