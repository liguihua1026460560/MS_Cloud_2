package com.macrosan.rabbitmq;/**
 * @author niechengxing
 * @create 2024-04-23 19:02
 */

import com.macrosan.utils.msutils.MsExecutor;
import com.macrosan.utils.msutils.MsThreadFactory;
import lombok.extern.log4j.Log4j2;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *@program: MS_Cloud
 *@description: 清除升级后旧版本创建的存储池队列（待残留的消息消费完后清除）
 *@author: niechengxing
 *@create: 2024-04-23 19:02
 */
@Log4j2
public class ClearMqQueue {
    private static final Scheduler clear = Schedulers.fromExecutor(new MsExecutor(1, 1, new MsThreadFactory("clear-queue")));
    public static Set<DeleteQueue> needClearQueue = new HashSet<>();
    public static Set<DeleteQueue> canDelete = new HashSet<>();

    public static void startClear() {
        log.info("begin clear old queue");
        clear.schedule(ClearMqQueue::clearQueue, 1, TimeUnit.MINUTES);
    }

    /**
     * 该方法可在所有队列初始化完成后执行,之后定时检测list中的队列是否为空
     */
    public static void clearQueue() {
        int messageCount;
        Iterator<DeleteQueue> iterator = needClearQueue.iterator();
        while (iterator.hasNext()) {
            DeleteQueue deleteQueue = iterator.next();
            log.info("wait clear queue:{}", deleteQueue.getQueue());
            try {
                messageCount = -1;
                messageCount = deleteQueue.getChannel().queueDeclarePassive(deleteQueue.getQueue()).getMessageCount();
                if (messageCount == 0) {
                    canDelete.add(deleteQueue);
                    iterator.remove();
                }
            } catch (IOException e) {
                log.debug("queue: {} is not existing", deleteQueue.getQueue());
                iterator.remove();
            }
        }
        Iterator<DeleteQueue> iterator0 = canDelete.iterator();
        while (iterator0.hasNext()) {
            DeleteQueue deleteQueue = iterator0.next();
            log.info("begin delete queue:{}", deleteQueue.getQueue());
            try {
                deleteQueue.getChannel().queueDelete(deleteQueue.getQueue());
                iterator0.remove();
            } catch (IOException e) {
                log.error("delete old queue {} error", deleteQueue.getQueue());
            }
        }

        if (needClearQueue.size() == 0 && canDelete.size() == 0) {
            log.info("clear old version queue successfully");
        } else {
            clear.schedule(ClearMqQueue::clearQueue, 1, TimeUnit.MINUTES);
        }
    }

}

