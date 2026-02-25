package com.macrosan.rabbitmq;/**
 * @author niechengxing
 * @create 2025-05-16 9:21
 */

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.SysConstants.REDIS_NODEINFO_INDEX;
import static com.macrosan.rabbitmq.RabbitMqUtils.getUUID;

/**
 *@program: MS_Cloud
 *@description: rabbitmq连接异常时用于检测并主动关闭自动重连机制
 *@author: niechengxing
 *@create: 2025-05-16 09:21
 */
@Log4j2
public class AutoRecoveryShutdown {
    private static final Scheduler RECOVER_SCHEDULER = Schedulers.fromExecutor(new MsExecutor(4, 1, new MsThreadFactory("rabbitmq-recovery-check")));

    public static void checkNeedRecover(String ip, Connection connection) {
        if (checkNeedRecover0(ip, connection)) {//如果节点未移除，则不关闭重连，保持定时任务检测
            RECOVER_SCHEDULER.schedule(() -> {
                checkNeedRecover(ip, connection);
            }, 30, TimeUnit.SECONDS);
        } else {
            //关闭之后不再检测
        }
    }

    public static boolean checkNeedRecover0(String ip, Connection connection) {
        log.debug("check need to recover:{}", getUUID(ip));
        if (StringUtils.isNotEmpty(getUUID(ip))) {
            if ("1".equals(RedisConnPool.getInstance().getCommand(REDIS_NODEINFO_INDEX).hget(getUUID(ip), "isRemoved"))) {
                if (connection != null) {
                    if (connection instanceof AutorecoveringConnection) {
                        log.info("is recoverConnect:{}", connection);
                        AutorecoveringConnection autoConn = (AutorecoveringConnection) connection;
                        try {
                            Class<? extends AutorecoveringConnection> aClass = autoConn.getClass();
                            Field manuallyClosedField = aClass.getDeclaredField("manuallyClosed");
                            manuallyClosedField.setAccessible(true);
                            Boolean manuallyClosed = (Boolean) manuallyClosedField.get(autoConn);
                            log.info("manuallyClosed:{}", manuallyClosed);
                            manuallyClosedField.set(autoConn, true);
                            Boolean manuallyClosed1 = (Boolean) manuallyClosedField.get(autoConn);
                            log.info("manuallyClosedAfterSet:{}", manuallyClosed1);
                        } catch (Exception e) {
                            log.error("close autoRecoverConnection fail", e);
                        }
//                                        autoConn.abort(); // 终止恢复线程
                    }
//                                    conn.close();
                }

                //todo 直接终止连接
                log.info("abort conn hostName:{}, conn:{}", ip, connection);
                if (connection != null && connection.isOpen()) {
                    connection.abort();
                }
                return false;
//                            connection.abort();
            } else {
                return true;
            }
        } else {
            if ("1".equals(RedisConnPool.getInstance().getCommand(REDIS_NODEINFO_INDEX).hget(getUUID(ip), "isRemoved"))) {
                if (connection != null) {
                    if (connection instanceof AutorecoveringConnection) {
                        log.info("is recoverConnect:{}", connection);
                        AutorecoveringConnection autoConn = (AutorecoveringConnection) connection;
                        try {
                            Class<? extends AutorecoveringConnection> aClass = autoConn.getClass();
                            Field manuallyClosedField = aClass.getDeclaredField("manuallyClosed");
                            manuallyClosedField.setAccessible(true);
                            Boolean manuallyClosed = (Boolean) manuallyClosedField.get(autoConn);
                            log.info("manuallyClosed:{}", manuallyClosed);
                            manuallyClosedField.set(autoConn, true);
                            Boolean manuallyClosed1 = (Boolean) manuallyClosedField.get(autoConn);
                            log.info("manuallyClosedAfterSet:{}", manuallyClosed1);
                        } catch (Exception e) {
                            log.error("close autoRecoverConnection fail", e);
                        }
//                                        autoConn.abort(); // 终止恢复线程
                    }
//                                    conn.close();
                }

                //todo 直接终止连接
                log.info("abort conn hostName:{}, conn:{}", ip, connection);
                if (connection != null && connection.isOpen()) {
                    connection.abort();
                }
//                            connection.abort();
            } else {
                if (connection != null && connection.isOpen()) {
                    try {
                        connection.close();
                    } catch (IOException e) {
                        log.error("conn hostName:{} close fail", connection.getAddress().getHostName());
                    }
                }
            }
            return false;
        }
    }
}

