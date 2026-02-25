package com.macrosan.utils.msutils;

import lombok.extern.log4j.Log4j2;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class UnsafeUtils {
    public static final Unsafe unsafe;

    static {
        Unsafe unsafe0;

        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe0 = (Unsafe) field.get(null);
        } catch (Exception e) {
            unsafe0 = null;
            log.info("load unsafe error", e);
            System.exit(1);
        }

        unsafe = unsafe0;
    }

}
