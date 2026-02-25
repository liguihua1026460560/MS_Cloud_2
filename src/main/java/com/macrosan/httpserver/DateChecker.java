package com.macrosan.httpserver;

import com.macrosan.constants.ErrorNo;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.property.PropertyReader;
import io.vertx.reactivex.core.AbstractVerticle;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static com.macrosan.constants.ServerConstants.DATE;
import static com.macrosan.constants.ServerConstants.EXPIRES;
import static com.macrosan.constants.SysConstants.PUBLIC_CONF_FILE;
import static com.macrosan.httpserver.ResponseUtils.responseError;

/**
 * DateChecker
 *
 * @author liyixin
 * @date 2019/4/3
 */
public class DateChecker extends AbstractVerticle {

    private static final long MAX_DELAY_TIME = 900 * 1000L;

    private static final long GMT_TO_LOCAL = 8 * 60 * 60 * 1000L;

    @Getter
    private static long currentTime = System.currentTimeMillis();

    private static final Logger logger = LogManager.getLogger(DateChecker.class.getName());

    private static String dateConf = null;

    @Override
    public void start() {
        PropertyReader conf = new PropertyReader(PUBLIC_CONF_FILE);
        dateConf = conf.getPropertyAsString("date_check");
        ScheduledThreadPoolExecutor scheduledThreadPool = new ScheduledThreadPoolExecutor(1, runnable -> new Thread(runnable, "timeUpdater"));
        scheduledThreadPool.scheduleAtFixedRate(() -> currentTime = System.currentTimeMillis(), 0, 1000_00, TimeUnit.NANOSECONDS);
    }

    static Predicate<? super MsHttpRequest> checkDateHandler() {
        return request -> {
            if ("0".equals(dateConf)) {
                return true;
            }

            String date = Optional.ofNullable(request.getHeader(DATE)).orElseGet(() -> request.getHeader("x-amz-date"));
            if (StringUtils.isNotBlank(date) &&
                    Math.abs(MsDateUtils.dateToStamp(date) - currentTime + GMT_TO_LOCAL) > MAX_DELAY_TIME) {
                if (request.getParam(EXPIRES) != null) {
                    return true;
                }

                logger.error("The request time is not match , request time : {} , current time : {}", date, MsDateUtils.stampToGMT(currentTime));
                responseError(request, ErrorNo.TIME_TOO_SKEWED);
                return false;
            }
            return true;
        };
    }
}
