package com.macrosan.constants;

import static com.macrosan.constants.ServerConstants.DEFAULT_DATA;

/**
 * AccountConstants
 * <p>
 * 记录账户相关常量
 *
 * @author liyixin
 * @date 2019/1/17
 */
public class AccountConstants {

    private AccountConstants() {
    }

    /**
     * 默认ak
     */
    public static final String DEFAULT_ACCESS_KEY = "aaaa1111bbbb2222cccc";

    /**
     * 默认sk
     */
    public static final String DEFAULT_SECRET_KEY = "aaaa1111bbbb2222cccc3333dddd4444eeee5555";

    /**
     * 默认用户id 000000000000
     */
    public static final String DEFAULT_USER_ID = "000000000000";

    /**
     * 默认用户名
     */
    public static final String DEFAULT_USER_NAME = "MOSSAnonymousAccount";

    /**
     * 默认管理账户id
     */
    public static final String DEFAULT_MGT_USER_ID = "admin";

    /**
     * 默认管理账户名
     */
    public static final String DEFAULT_MGT_USER_NAME = "admin";

    /**
     * 默认Signature
     */
    public static final byte[] DEFAULT_SIGNATURE = DEFAULT_DATA;
}
