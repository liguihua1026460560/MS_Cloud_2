package com.macrosan.utils.msutils;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;


/**
 * 描述：修改日志级别  所有级别均支持，配合 change_log_level_MS_Cloud.py 使用
 */
public class ChangeLogLevel {


    private static final Logger logger = LogManager.getLogger(ChangeLogLevel.class);

    private final static String OFF = "off";
    private final static String FATAL = "fatal";
    private final static String ERROR = "error";
    private final static String WARN = "warn";
    private final static String INFO = "info";
    private final static String DEBUG = "debug";
    private final static String TRACE = "trace";
    private final static String ALL = "all";

    public ChangeLogLevel() {
    }

    /**
     * 描述 动态修改日志级别
     **/
    public static void changeLogLevelM(String[] level) {

        String level2 = level[0];
        switch (level2) {
            case OFF:
                setLogLevel(Level.OFF);
                break;
            case FATAL:
                setLogLevel(Level.FATAL);
                break;
            case ERROR:
                setLogLevel(Level.ERROR);
                break;
            case WARN:
                setLogLevel(Level.WARN);
                break;
            case INFO:
                setLogLevel(Level.INFO);
                break;
            case DEBUG:
                setLogLevel(Level.DEBUG);
                break;
            case TRACE:
                setLogLevel(Level.TRACE);
                break;
            case ALL:
                setLogLevel(Level.ALL);
                break;
            default:
                logger.info("please input right param ");
                break;

        }

    }


    /**
     * @Description 设置日志的级别
     **/
    private static void setLogLevel(Level level) {

        logger.info("set log level to : " + level.name());

        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = context.getConfiguration();
        LoggerConfig rootLoggerConfig = configuration.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        rootLoggerConfig.setLevel(level);
        context.updateLoggers();

    }

}
