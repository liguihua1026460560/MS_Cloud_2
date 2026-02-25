package com.macrosan.database.redis;

import com.macrosan.database.StatusEnum;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.utils.msutils.MsException;
import io.lettuce.core.ConnectionFuture;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.codec.StringCodec;
import io.vertx.reactivex.core.Vertx;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.list.mutable.FastList;
import reactor.core.Disposable;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;

import static com.macrosan.utils.functional.exception.Sleep.sleep;

/**
 * SharedConnection
 * Shared连接的实体类,使用的JDK8改进的StampedLock中的读写锁
 *
 * @author liyixin
 * @date 2018/11/27
 */
@Log4j2
public class SharedConnection extends AbstractConnection {

    private Logger logger = LogManager.getLogger(SharedConnection.class);

    private StampedLock lock;

    private int index = ThreadLocalRandom.current().nextInt(1, 3);

    /**
     * failOver发生时的时间，两次failOver间隔不能小于5s
     */
    private long stamp = 0;

    /**
     * 定时器的ID，-1表示定时器未开启
     */
    private long timerId = -1;

    /**
     * 主连接，要用到的时候再发起连接，断开后也不会直接开始重连，而是下次使用时开始重连
     */
    private StatefulRedisConnection<String, String> master;

    /**
     * 从连接，因为使用很频繁所以一直维持
     */
    private StatefulRedisConnection<String, String> slave;

    /**
     * 标示master是否可用，而不是整个逻辑连接是否可用，因为slave基本不会不可用
     */
    private volatile boolean closed = true;

    /**
     * 共享连接在初始化的时候就全部连上
     *
     * @param client redis客户端
     * @param nodes  节点
     */
    SharedConnection(RedisClient client, FastList<RedisURI> nodes) {
        super(client, nodes);
        createConnection();
        this.lock = new StampedLock();
        initCache(nodes.get(0).getDatabase());
    }

    @Override
    public RedisReactiveCommands<String, String> reactive() {
        return cacheReactive();
    }


    @Override
    void reconnect() {
        long stamp = lock.writeLock();
        if (status == StatusEnum.CLOSED) {
            super.reconnect();
        }
        lock.unlock(stamp);
    }

    private void active() {
        long stamp = lock.readLock();
        if (status == StatusEnum.IDLE) {
            status = StatusEnum.ALIVE;
        }
        lock.unlock(stamp);
    }

    @Override
    void dealStatus() {
        switch (status) {
            case ALIVE:
                break;
            case CLOSED:
                reconnect();
                break;
            case IDLE:
                active();
                break;
            default:
                break;

        }
    }

    @Override
    void createConnection() {
        this.slave = client.connect(nodes.get(0));
        this.subConnection = client.connectPubSub(nodes.get(0));
        nodes.get(0).getDatabase();
        this.status = StatusEnum.ALIVE;
    }

    private synchronized void failOver() {
        try {
            long currentTime = System.currentTimeMillis();
            if (currentTime - stamp > 5000) {
                stamp = currentTime;
                index = 3 - index;
                if (master != null) {
                    master.close();
                }
                master = client.connect(nodes.get(index));
                closed = false;
                logger.info("switch to {}", nodes.get(index));
            }
        } catch (RedisConnectionException e) {
            logger.error("switch to {} fail, start auto-reconnect", nodes.get(index));
            closed = true;
            Vertx vertx = ServerConfig.getInstance().getVertx();

            //切换失败就开始自动重连，两个端口来回尝试
            timerId = vertx.setPeriodic(1000, v -> {
                try {
                    //对另一个端口发起重连
                    index = 3 - index;
                    master = client.connect(nodes.get(index));
                    //重连成功后修改连接状态，关闭定时器，重置定时器ID，最后唤醒所有等待线程
                    closed = false;
                    vertx.cancelTimer(timerId);
                    timerId = -1;
                    this.notifyAll();
                } catch (RedisConnectionException ex) {
                    logger.error("Auto-Reconnect task: connect fail");
                }
            });
        }
    }

    @Override
    public void setMasterClosed() {
        closed = true;
    }

    @Override
    public StatefulRedisConnection<String, String> newMaster() {

        Thread t = Thread.currentThread();
        Disposable disposable = ErasureServer.DISK_SCHEDULER.schedule(() -> {
            log.info("thread interrupt: " + t.getName());
            t.interrupt();
        }, 1, TimeUnit.MINUTES);

        try {
            for (int i = 0; i < 10; i++) {
                try {
                    ConnectionFuture<StatefulRedisConnection<String, String>> future = client.connectAsync(StringCodec.UTF8, nodes.get(index));
                    return future.get();
                } catch (RedisConnectionException | ExecutionException e) {
                    index = 3 - index;
                } catch (Exception e) {
                    logger.error("", e);
                    throw new MsException(0, "connect time out", e);
                }
                sleep(1000);
            }
            throw new MsException(0, "connect time out");
        } finally {
            disposable.dispose();
        }
    }

    @Override
    public StatefulRedisConnection<String, String> master() {
        //如果连接是开着的，就直接返回,大多数情况下这里就返回了
        if (!closed) {
            return master;
        }

        //如果连接关闭且定时器未开启，就尝试连接
        try {
            if (timerId == -1) {
                //在同步块中连接，避免多线程调用造成重复连接
                synchronized (this) {
                    if (closed) {
                        master = client.connect(nodes.get(index));
                        closed = false;
                        logger.info("connect to {}", nodes.get(index));
                    } else {
                        return master;
                    }
                }
            }
        } catch (RedisConnectionException e) {
            logger.error("connect time out");
            failOver();
        }

        //再次检查连接状态，直接连接成功或者切换一次成功就直接返回，切换后还是失败就等待
        if (!closed) {
            return master;
        }

        //如果连接关闭，并且定时器也启动了，就等待直到连接状态变为true
        try {
            synchronized (this) {
                while (closed) {
                    this.wait(20000);
                }
            }
            return master;
        } catch (Exception e) {
            logger.error("wait time out,can not connect to redis", e);
            throw new MsException(0, "connect time out");
        }
    }

    @Override
    public StatefulRedisConnection<String, String> slave() {
        return slave;
    }

}
