package com.macrosan.utils.functional.exception;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ThreadFactory;

/**
 * Sleep
 *
 * @author liyixin
 * @date 2018/12/19
 */
public class Sleep {

    private static final Logger logger = LogManager.getLogger(Sleep.class.getName());

    private Sleep() {
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            logger.error("exception at sleep :", e);
//            Thread.currentThread().interrupt();
        }
    }
}
