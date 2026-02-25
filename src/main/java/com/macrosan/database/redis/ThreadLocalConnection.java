package com.macrosan.database.redis;

import com.macrosan.database.StatusEnum;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.list.mutable.FastList;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static com.macrosan.utils.functional.exception.ThrowingFunction.throwingFunctionWrapper;

/**
 * ThreadLocalConnection
 * ThreadLocal连接的实体类,使用的可重入锁 ReentrantLock,并且提供事务，分批，乐观锁等操作
 *
 * @author liyixin
 * @date 2018/11/28
 */
public class ThreadLocalConnection extends AbstractConnection {

    private static final Logger logger = LogManager.getLogger(ThreadLocalConnection.class.getName());

    private ReentrantLock lock;

    /**
     * 线程本地连接为惰性初始化，需要的时候才连接
     *
     * @param client redis客户端
     * @param nodes  节点
     */
    ThreadLocalConnection(RedisClient client, FastList<RedisURI> nodes) {
        super(client, nodes);
        this.status = StatusEnum.CLOSED;
        this.lock = new ReentrantLock();
    }

    private void active() {
        lock.lock();
        if (status == StatusEnum.IDLE) {
            status = StatusEnum.ALIVE;
        }
        lock.unlock();
    }

    @Override
    void dealStatus() {
        switch (status) {
            case ALIVE:
                return;
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

    }

    /**
     * 批量执行命令的方法
     *
     * @param dataBase    目标数据库
     * @param commandList 命令列表
     * @return 操作结果
     */
    public FastList<Object> batchOperation(int dataBase, List<Command<?>> commandList) {
        try {
            super.sync().select(dataBase);
            super.async().setAutoFlushCommands(false);
            FastList<RedisFuture> resList = new FastList<>(commandList.size());
            commandList.forEach(command -> resList.add(command.apply()));
            super.async().flushCommands();
            LettuceFutures.awaitAll(30, TimeUnit.SECONDS, resList.toArray(new RedisFuture[0]));
            super.async().setAutoFlushCommands(true);

            return resList.stream()
                    .map(throwingFunctionWrapper(Future::get))
                    .collect(FastList::new, FastList::add, FastList::addAll);
        } catch (Exception e) {
            logger.error("Batch Operation fail :", e);
            return new FastList<>(0);
        }
    }

    /**
     * 批量执行命令的方法
     *
     * @param dataBase     目标数据库
     * @param commandArray 命令数组
     * @return 操作结果
     */
    public FastList<Object> batchOperation(int dataBase, Command<?>... commandArray) {
        return batchOperation(dataBase, FastList.newListWith(commandArray));
    }

    /**
     * 执行事务的方法
     *
     * @param dataBase    目标数据库
     * @param commandList 命令列表
     * @return 操作结果
     */
    @Nullable
    public TransactionResult transaction(int dataBase, List<Command<?>> commandList) {
        try {
            super.sync().select(dataBase);
            super.async().multi();
            commandList.forEach(Command::apply);
            RedisFuture<TransactionResult> exec = super.async().exec();
            TransactionResult res = exec.get();
            if (res.wasDiscarded()) {
                logger.error("Transaction was discarded");
                return null;
            }
            return res;
        } catch (Exception e) {
            logger.error("transaction fail :", e);
            return null;
        }
    }

    /**
     * 执行事务的方法
     *
     * @param dataBase     目标数据库
     * @param commandArray 命令数组
     * @return 操作结果
     */
    @Nullable
    public TransactionResult transaction(int dataBase, Command<?>... commandArray) {
        return transaction(dataBase, FastList.newListWith(commandArray));
    }

    /**
     * 执行带乐观锁的事务操作
     *
     * @param dataBase    目标数据库
     * @param commandList 命令列表
     * @param keys        要观察的键（可以是多个）
     * @return 操作结果
     */
    @Nullable
    public TransactionResult optimisticLock(int dataBase, List<Command<?>> commandList
            , String... keys) {
        super.sync().watch(keys);
        TransactionResult res = transaction(dataBase, commandList);
        super.sync().unwatch();
        return res;
    }

    /**
     * 执行带乐观锁的事务操作
     *
     * @param dataBase     目标数据库
     * @param commandArray 命令数组
     * @param key          要观察的键（只能是一个）
     * @return 操作结果
     */
    @Nullable
    public TransactionResult optimisticLock(int dataBase, String key, Command<?>... commandArray) {
        return optimisticLock(dataBase, FastList.newListWith(commandArray), key);
    }

    @Override
    public void setMasterClosed() {
        throw new UnsupportedOperationException();
    }

    @Override
    public StatefulRedisConnection<String, String> newMaster() {
        throw new UnsupportedOperationException();
    }

    @Override
    public StatefulRedisConnection<String, String> master() {
        throw new UnsupportedOperationException();
    }

    @Override
    public StatefulRedisConnection<String, String> slave() {
        throw new UnsupportedOperationException();
    }
}
