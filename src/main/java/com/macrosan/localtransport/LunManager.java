package com.macrosan.localtransport;

import com.macrosan.httpserver.ServerConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * LunManager
 *
 * @author liyixin
 * @date 2019/10/14
 */
@Log4j2
public class LunManager {

    private static final IntObjectHashMap<LunInfo> lunInfos = new IntObjectHashMap<>();

    private static final AtomicInteger WIP = new AtomicInteger(0);

    public static boolean isAvailable(String lun) {
        return getLunInfo(lun).isAvailable();
    }

    /**
     * 设置lun的可用性，这里仅会存入对端的Lun，在导入Lun时由LocalServer调用
     * <p>
     * note : 该方法只会被一个线程调用（local transport的eventLoop），所以不用保证线程安全
     *
     * @param lun     lun 名字
     * @param ability 可用性
     */
    public static void setLunAbility(String lun, boolean ability) {
        int key = hashcode(lun);
        if (lunInfos.containsKey(key)) {
            lunInfos.get(key).setAvailable(ability);
        } else {
            lunInfos.put(key, new LunInfo(lun, false, ability));
        }
    }

    /**
     * 获取Lun信息
     * <p>
     * 这里仅会存入本端的Lun。
     *
     * @param lun lun名字
     * @return Lun信息
     */
    public static LunInfo getLunInfo(String lun) {
        final int key = hashcode(lun);
        //fast-path
        if (lunInfos.containsKey(key)) {
            return lunInfos.get(key);
        }

        //FILO的顺序将并行修改串行化
        int order = WIP.incrementAndGet();
        for (; ; ) {
            if (WIP.get() == order) {
                boolean isLocal = lun.contains(ServerConfig.getInstance().getSp());
                final LunInfo lunInfo = new LunInfo(lun, isLocal, true);
                lunInfos.put(key, lunInfo);
                WIP.decrementAndGet();
                return lunInfo;
            }
        }
    }

    /**
     * 以lun名字最后的下标当做hashcode，（eg: fs-SP1-0的下标是0）
     * 这样的hashcode散列均匀且速度更快
     *
     * @param lun lun名字
     * @return lun名字的hashcode，用作hashMap的key
     */
    private static int hashcode(String lun) {
        int index = 0;
        int factor = 1;
        for (int i = lun.length() - 1, j = 0; i >= j; i--) {
            char c = lun.charAt(i);
            if (c == '-') {
                return index;
            }
            index = index + charToInt(c) * factor;
            factor = factor * 10;
        }

        log.info("lunName's syntax is wrong: {}", lun);
        return index;
    }

    /**
     * 将ASCII字符转化为数字
     * <p>
     * note: 未判断范围，即字母输入也有输出
     *
     * @param c 字符
     * @return 数字
     */
    private static int charToInt(char c) {
        return c - '0';
    }

    public static final class LunInfo extends AtomicInteger {

        private final boolean isLocal;

        @Setter
        @Getter
        private String lunName;

        @Setter
        @Getter
        private volatile boolean isAvailable;

        private volatile Runnable hook;

        private LunInfo(String lunName, boolean isLocal, boolean isAvailable) {
            this.isLocal = isLocal;
            this.isAvailable = isAvailable;
            this.lunName = lunName;
        }

        public void addListener(Runnable hook) {
            this.hook = hook;
            if (get() == 0) {
                hook.run();
                this.hook = null;
            }
        }

        public void decrease() {
            if (!isLocal) {
                final int wip = decrementAndGet();
                if (wip == 0 && hook != null) {
                    hook.run();
                }
            }
        }

        public void increase() {
            if (!isLocal) {
                int wip = incrementAndGet();
            }
        }
    }
}
