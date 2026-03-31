package com.macrosan.constants;

/**
 * 错误码
 *
 * @author Jiangmin
 * @date 2017年8月11日 上午11:32:13
 */
public class ErrorNo {

    private ErrorNo() {
    }

    /**
     * 正确成功返回状态
     */
    public static final int SUCCESS_STATUS = 1;

    /**
     * 未知错误
     */
    public static final int UNKNOWN_ERROR = -1;

    /*-------------------socket信息相关错误码， 以 -1开头-----------------*/

    /**
     * socket信息传输错误
     */
    public static final int SOCKET_MSG_ERROR = -101;

    /**
     * socket开启监听失败
     */
    public static final int SOCKET_LISTEN_ERROR = -102;

    /**
     * redis连接失败
     */
    public static final int REDIS_CONNECT_ERROR = -103;

    /**
     * mongodb连接失败
     */
    public static final int MONGODB_CONNECT_ERROR = -104;

    /**
     * mongodb命令失败
     */
    public static final int MONGODB_COMMAND_ERROR = -105;

    /**
     * 客户端时间和MOSS端时间的时间差过大
     */
    public static final int TIME_TOO_SKEWED = -106;


    /*----------------bucket相关错误码， 以 -2开头--------------------------*/

    /**
     * bucket数量太多
     */
    public static final int TOO_MANY_BUCKETS = -201;

    /**
     * bucket已经存在
     */
    public static final int BUCKET_EXISTS = -202;

    /**
     * bucket不存在
     */
    public static final int NO_SUCH_BUCKET = -205;

    /**
     * bucket下存在对象
     */
    public static final int OBJ_EXIST = -213;

    public static final int OBJ_EXIST_0 = -214;

    public static final int OBJ_EXIST_1 = -215;

    public static final int OBJ_EXIST_2 = -216;

    /**
     * 修改桶操作权限，输入id不存在
     */
    public static final int NO_SUCH_ID = -218;

    /*
     * bucket NFS未关闭
     * */
    public static final int NFS_NOT_STOP = -220;

    /**
     * 开启了ES
     */
    public static final int NFS_CONFLICT = -221;

    /**
     * 挂载太多的NFS
     */
    public static final int NFS_MOUNT_TOO_MANY = -222;

    /**
     * refer不匹配
     */
    public static final int REFERER_NOT_ALLOWED = -223;
    /**
     * refer配置超过限制
     */
    public static final int TOO_MANY_REFERER = -224;

    /**
     * 桶没有进行防盗链配置
     */
    public static final int BUCKET_REFERER_NOT_EXIST = -225;
    /**
     * 桶没有开启软配额配置
     */
    public static final int NO_SOFT_QUOTA = -226;
    public static final int CORS_RULE_TOO_LONG = -227;
    public static final int NO_BUCKET_TAGGING = -228;

    /**
     * 桶拥有者没有配置文件账户
     **/
    public static final int BUCKET_OWNER_MISSING_FS_ACCOUNT = -229;

    /*-----------------------vnode相关错误码， 以 -3开头----------------------*/


    /*---------------------------用户相关错误码， 以 -4开头-----------------------*/

    /**
     * 账户不存在
     */
    public static final int ACCOUNT_NOT_EXISTS = -415;
    /**
     * 用户密码验证失败
     */
    public static final int ACCESS_DENY = -408;

    /**
     * 拒绝访问
     */
    public static final int ACCESS_FORBIDDEN = -410;

    /**
     * Sign不匹配
     */
    public static final int SIGN_NOT_MATCH = -409;

    /**
     * AK不匹配
     */
    public static final int AK_NOT_MATCH = -416;

    /**
     * 自定义AK不匹配
     */
    public static final int ACCESSKEY_NOT_MATCH = -417;

    /**
     * 自定义SK不匹配
     */
    public static final int SECRETKEY_NOT_MATCH = -418;

    /**
     * 必须参与签名的头未签名
     */
    public static final int HEADERS_PRESENT_NOT_SIGNED = -420;
    /**
     * v4 header中存在 Authorization 无Date或X-Amz-Date
     */
    public static final int AUTH_REQUIRE_DATE = -421;
    /**
     * v4 无x-amz-content-sha-256 参数
     */
    public static final int MISSING_X_AMZ_CONTENT_SHA_256 = -422;
    /**
     * 不支持的认证类型
     */
    public static final int UNSUPPORTED_AUTHORIZATION_TYPE = -423;

    /**
     * 请求过期
     */
    public static final int EXPIRED_REQUEST = -424;


    /**
     * v4 Authorization 格式不正确
     */
    public static final int AUTHORIZATION_HEADER_MALFORMED = -425;
    /**
     * v4 Authorization credential date 与 Date 不一致 400
     */
    public static final int AUTHORIZATION_HEADER_DATE_MALFORMED = -426;

    /**
     * v4 Authorization 字段不正确
     */
    public static final int AUTHORIZATION_HEADER_FIELD_MALFORMED = -427;
    /**
     * v4 Authorization 字段不正确
     */
    public static final int AUTHORIZATION_HEADER_SERVICE_MALFORMED = -428;
    /**
     * hash 不匹配
     */
    public static final int X_AMZ_CONTENT_SHA_256_MISMATCH = -429;
    /**
     * 签名不匹配  后端返回用
     */
    public static final int SIGNATURE_DOES_NOT_MATCH = -430;
    /**
     * v4 Authorization credential date 格式不正确
     */
    public static final int AUTHORIZATION_HEADER_DATE_FORMAT_MALFORMED = -431;
    /**
     * x-amz-content-sha256 值非法
     */
    public static final int X_AMZ_CONTENT_SHA_256_INVALID = -432;

    /**
     * 分块上传时 x-amz-decoded-content-length不存在或非法
     */
    public static final int MISSING_DECODED_CONTENT_LENGTH = -433;
    /**
     * 使用http chunk上传对象或分片 v2或v4单块传输暂不支持
     */
    public static final int TRANSFER_ENCODING_NOT_IMPLEMENTED = -434;
    /**
     * AUTHORIZATION 格式错误
     */
    public static final int AUTHORIZATION_INVALID = -435;
    /**
     * 分块传输时块结束符(\r\n)错误
     */
    public static final int INCOMPLETE_BODY = -436;
    /**
     * 临时URL v4 查询参数中的签名算法错误
     */
    public static final int AUTHORIZATION_ALGORITHM_QUERY_PARAMETERS_ERROR = -437;
    /**
     * 临时URL v4 查询参数中的有效时间错误不是数字
     */
    public static final int AUTHORIZATION_EXPIRES_QUERY_PARAMETERS_ERROR = -438;
    /**
     * 临时URL v4 查询参数中的有效时间值错误
     */
    public static final int AUTHORIZATION_EXPIRES_QUERY_PARAMETERS_INVALID_ERROR = -439;

    /**
     * 接口添加非必要请求体
     */
    public static final int UNEXPECTEDCONTENT = -440;

    /**
     * 缺少必要请求头
     */
    public static final int MISSING_SECURITY_HEADER = -441;

    /*-------------------------对象相关错误码， 以 -5开头-----------------------*/

    /**
     * 对象不存在
     */
    public static final int NO_SUCH_OBJECT = -502;

    /**
     * 没有bucket操作权限 或bucket操作权限低
     */
    public static final int NO_BUCKET_PERMISSION = -505;

    /**
     * 分片任务不存在，已经完成删除或者尚未初始化
     */
    public static final int NO_SUCH_UPLOAD_ID = -507;

    /**
     * 太多未完成的分片任务
     */
    public static final int TOO_MANY_INCOMPLETE_UPLOAD = -508;

    /**
     * 用户元数据大于2KB
     */
    public static final int META_DATA_TOO_LARGE = -511;

    /**
     * part容量到达上限
     */
    public static final int OBJECT_SIZE_OVERFLOW = -510;

    /**
     * part容量到达上限
     */
    public static final int PART_USED_SIZE_OVER = -512;

    /**
     * 设置Bucket权限出错，没有这样的权限
     */
    public static final int NO_SUCH_BUCKET_PERMISSION = -513;

    /**
     * 设置Object权限出错，没有这样的权限
     */
    public static final int NO_SUCH_OBJECT_PERMISSION = -514;

    /**
     * nfs和数据同步冲突
     */
    public static final int NFS_SYNC_CONFLICTS = -519;

    /**
     * 删除对象失败
     */
    public static final int DEL_OBJ_ERR = -521;

    /**
     * 多段上传时非最后一段的其他段长度小于100KB
     */
    public static final int TOO_SMALL_PART = -522;

    /**
     * 待合并的段无效
     */
    public static final int PART_INVALID = -523;
    /**
     * xml反序列化错误
     */
    public static final int MALFORMED_XML = -524;
    /**
     * 合并时用户非多段任务的发起者
     */
    public static final int PART_USER_NOT_MATCH = -525;

    /**
     * 参数不合法
     */
    public static final int INVALID_ARGUMENT = -526;

    /**
     * MD5不正确
     */
    public static final int INVALID_MD5 = -527;

    /**
     * 请求不合法
     */
    public static final int INVALID_REQUEST = -528;

    /**
     * range参数不合法
     */
    //-529=InvalidRange/Requested Range Not Satisfiable./416
    public static final int INVALID_RANGE = -529;
    /**
     * 参数不合法
     */
    public static final int MISSING_CONTENT_LENGTH = -530;

    /**
     * 批量删除对象数目超过1000
     */
    public static final int TOO_MANY_OBJECTS = -531;

    /**
     * 消息体长度超过2M
     */
    public static final int REQUEST_BODY_TOO_LONG = -532;

    /**
     * 批量删除传入空对象
     */
    public static final int INPUT_EMPTY_OBJECT = -533;

    /**
     * 对象被保护
     */
    public static final int OBJECT_IMMUTABLE = -535;

    /**
     * tagging参数不合法
     */
    public static final int INVALID_TAGGING_ARGUMENT = -536;

    public static final int NO_SUCH_VERSION = -538;
    /**
     * part merge partList参数未按partNumber升序传入
     */
    public static final int INVALED_PART_ORDER = -534;
    /**
     * 桶元数据开关未开
     */
    public static final int BUCKET_SWITCH_OFF = -537;

    /**
     * 参数不合法
     */
    public static final int VERSION_ID_MARKER_ERROR = -539;
    /**配置文件错误码，以-6开头*/

    /**
     * ES超出buffer
     */
    public static final int ES_SEARCH_BUFFER_EXCEEDED = -540;

    /**
     * 使用http chunk上传对象或分片，不传递CONTENT_LENGTH
     */
    public static final int CONTENT_LENGTH_NOT_ALLOWED = -541;
    public static final int GET_OBJECT_CANCELED = -543;

    public static final int GET_OBJECT_TIME_OUT = -545;
    /**
     * 请求体为空
     */
    public static final int MISSING_REQUEST_BODY = -546;

    public static final int INVALID_MODEL = -547;

    public static final int NO_SUCH_CLEAR_MODEL_CONFIGURATION = -548;

    public static final int EMPTY_TIME = -549;

    public static final int INVALID_TIME = -550;

    public static final int INVALID_REMAINING = -551;
    public static final int INVALID_TAG = -552;
    public static final int EMPTY_TAG = -553;

    public static final int NO_CORS_CONFIGURATION = -554;

    public static final int MISSING_FILE_LENGTH = -555;

    /**
     * 字符串输入错误，以-7开头
     */
    public static final int NAME_INPUT_ERR = -701;

    public static final int BUCKET_NAME_INPUT_ERR = -703;

    public static final int BUCKET_QUOTA_INPUT_ERR = -704;

    public static final int ACCOUNT_NAME_INPUT_ERR = -705;

    /**
     * 账户配额的类型不合法
     */
    public static final int NO_SUCH_PERF_TYPE = -706;

    /*-------------节点相关错误，以-8开头---------------------*/


    /*--------------zfs相关错误码， 以 -9开头-----------------*/


    /*----------------dns相关错误码， 以 -10开头---------------*/

    public static final int TAKEOVER_LUN_NOT_AVAILABLE = -1104;

    /*接管恢复相关错误码， 以 -12开头*/

    /*change_map_info相关错误码， 以 -13开头*/

    /*-------------------mongodb相关错误码， 以 -14开头----------------*/
    /*	通知启动掉线端对端mongodb_takeover错误*/

    /*-------------------请求上线相关返回码， 以 -15开头----------------*/

    /**
     * --------------------桶配额相关错误码-------------------------
     */
    public static final int NO_ENOUGH_SPACE = -1901;

    public static final int NO_ENOUGH_OBJECTS = -1902;

    public static final int EXIST_CLEAN_MODEL_CONFIGURATION = -1903;

    //桶生命周期错误码以-20开头
    public static final int MALFORMED_ERROR = -2001;

    public static final int NO_LIFECYCLE_CONFIGURATION = -2002;
    //生命周期id长度超过255个字符
    public static final int ID_LENGTH_EXCEED = -2003;

    public static final int INVALID_DATE = -2004;

    public static final int INVALID_LIFECYCLE_PREFIX = -2005;
    // 无效存储策略
    public static final int INVALID_STORAGE_CLASS = -2006;
    // 时间重复
    public static final int LIFECYCLE_INVALID_REQUEST = -2007;
    // 过期小于迁移
    public static final int LIFECYCLE_INVALID_ARGUMENT = -2008;
    // 前缀冲突
    public static final int LIFECYCLE_INVALID_PREFIX = -2009;
    // days和date标签冲突
    public static final int LIFECYCLE_MIX_DATE_AND_DAYS = -2010;

    public static final int LIFECYCLE_PREFIX_CONFICT = -2011;

    public static final int INVALID_LIFECYCLE_RULE = -2012;

    public static final int INVALID_LIFECYCLE_TAG = -2013;

    public static final int LIFECYCLE_INVALID_ID = -2014;

    public static final int RULE_CONFLICT = -2015;

    public static final int INVALID_LIFECYCLE_PREFIX_CHAR = -2016;

    public static final int LIFECYCLE_INVALID_ID_CHAR = -2017;

    public static final int INVALID_SOURCE_CLUSTER = -2018;

    public static final int INVALID_BACKUP_CLUSTER = -2019;

    public static final int INVALID_BACKUP_RULE = -2020;


    //copy object error code
    public static final int PRECONDITION_FAILED = -2201;

    public static final int SOURCE_TOO_LARGE = -2202;

    /**
     * ----------------------------桶清单相关的错误码------------------------------------
     */

    public static final int NO_INVENTORY_CONFIGURATION = -2203;

    public static final int TOO_MANY_CONFIGURATIONS = -2204;

    public static final int INVALID_INVENTORY_ID = -2205;

    // Schedule Frequency cannot be empty and can only be Daily or Weekly.
    public static final int INVALID_SCHEDULE = -2206;

    // OptionField contains an invalid field
    public static final int INVALID_OPTIONAL_FIELDS = -2207;

    // IncludeObjectVersions cannot be empty and can only be All or Current
    public static final int INVALID_OBJECT_VERSIONS = -2208;

    // The inventory file format is invalid
    public static final int INVALID_INVENTORY_FORMAT = -2209;

    //The specified account ID is not the owner of the target bucket
    public static final int INVALID_INVENTORY_ACCOUNT_ID = -2210;

    public static final int NO_SUCH_DEST_BUCKET = -2211;

    public static final int NO_SPECIFIED_DEST_BUCKET = -2212;

    public static final int PrefixExistInclusionRelationship = -2213;

    public static final int InvalidS3DestinationBucketPrefix = -2214;

    public static final int InvalidFilterPrefix = -2215;

    public static final int InvalidS3DestinationBucket = -2216;

    public static final int InvalidInventoryType = -2217;

    public static final int InvalidInventory = -2218;

    /**
     * --------------------------异常处理相关错误码------------------------------------
     */
    public static final Integer REPEATED_KEY = -3001;

    public static final int GET_OBJECT_BYTES_FAILED = -3002;

    /***
     * ------------------------多版本相关错误码--------------------------
     */
    public static final int InvalidBucketState = -2301;

    public static final int METHOD_NOT_ALLOWED = -2302;

    public static final int VERSION_LIMIT = -2308;

    public static final int InvalidBucketVersionQuota = -2309;

    /**
     * -----------------------worm相关错误码----------------------------
     */

    public static final int WORM_STATE_CONFLICT = -2303;
    public static final int BUCKET_MISSING_OBJECT_LOCK_CONFIG = -2304;
    public static final int OBJECT_RETAIN_UNTIL_DATE_ERROR = -2305;
    public static final int NO_SUCH_OBJECT_RETENTION = -2306;
    public static final int INVALID_OBJECT_LOCK_REQUEST = -2307;
    public static final int NO_OBJECT_LOCK_CONFIGURATION = -4001;

    /**
     * ---------------------桶日志相关错误码-----------------------------
     */

    public static final int PREFIX_LENGTH_ILLEGAL = -2411;
    public static final int TARGET_BUCKET_DELETED = -2412;
    public static final int TARGET_BUCKET_EXIST = -2413;
    public static final int NOT_IN_SAME_REGION = -2414;

    /**
     * ---------------------桶通知相关错误码-----------------------------
     */

    public static final int SEND_TEST_MESSAGE_ERROR = -2401;
    public static final int TOPIC_OR_QUEUE_NOT_EXIST = -2402;
    public static final int EVENT_INPUT_ERROR = -2403;
    public static final int CREATE_CHANNEL_FAIL = -2404;
    public static final int EXCHANGE_NOT_EXIST = -2405;
    public static final int PARAM_IS_ILLEGAL = -2406;
    public static final int THE_MRN_ILLEGAL = -2407;
    public static final int CONNECTED_FAIL = -2408;
    public static final int EVENT_REPETITION = -2409;
    public static final int FILTER_NAME_ERROR = -2410;


    /**
     * ---------------------桶回收站相关错误码--------------------------
     */

    public static final int TRASH_NAME_TOO_LONG = -2420;
    public static final int INVALID_TRASH_DIRECTORY_NAME = -2421;
    public static final int MALFORMED_JSON = -2422;
    public static final int NO_SUCH_TRASH_DIRECTORY = -2423;
    public static final int BUCKET_OPEN_VERSION = -2424;

    /**
     * ------------------------桶配置加密相关错误码----------------------
     */
    public static final int SSE_ALGORITHM_INPUT_ERROR = -2415;
    public static final int BUCKET_ENCRYPTION_NOT_FOUND = -2416;
    public static final int INVALID_ENCRYPTION_ALGORITHM_ERROR = -2417;
    public static final int INVALID_ENCRYPTION_STATEMENT_ERROR = -2418;

    /**
     * ------------------------桶配置加密相关错误码----------------------
     */
    public static final int THE_SETTING_ERROR = -2501;
    public static final int TIME_FORMAT_ERROR = -2502;


    /**
     * ---------------------iam 相关错误码-------------------------
     */

    /**
     * 用户相关错误码
     **/
    public static final int USER_NOT_EXISTS = 101; // 用户不存在
    public static final int USER_NAME_INPUT_ERR = 102; //用户输入的用户名错误
    public static final int TOO_MANY_USER = 103; // 账号下用户数量达到上限
    public static final int USER_EXISTED = 104; // 用户已存在
    public static final int TOO_MANY_GROUP_IN_USER = 105; // 用户可加入的用户组达到数量上限
    public static final int INVAILD_PASSWD = 106; // 密码无效
    public static final int DENY_PROGRAM_ACCESS = 107; // 不允许编程访问时不能创建ak
    public static final int USER_ADDED_GROUP = 108; // 用户加入了组
    public static final int USER_ATTACH_POLICY = 109; // 用户附加了策略
    public static final int USER_PASSWORD_ERROR = 110; // 用户密码错误
    public static final int PASSWORD_INPUT_ERR = 111;//账户和用户输入的密码错误
    public static final int USER_PASSWORD_NOT_EXISTS = 112;//用户密码不存在

    /**
     * AK相关错误码
     **/
    public static final int TOO_MANY_AK = 201; // 用户的ak数量达到上限
    public static final int AK_NOT_EXISTS = 202; // ak不存在
    public static final int DELETE_AK_FAIL = 203; // 删除ak失败
    public static final int AK_EXISTS = 204; // ak存在
    public static final int INVALID_AK_ACL = 205; // ak存在

    /**
     * 锁错误码
     **/
    public static final int RESOURCE_LOCKED = 301; // 请求的资源已经被加锁

    /**
     * 策略相关错误码
     **/
    public static final int POLICY_EXISTED = 401; // 策略已经存在
    public static final int POLICY_NOT_EXISTS = 402; // 策略不存在
    public static final int TOO_MANY_POLICY = 403; // 策略数量达到上限
    public static final int POLICY_TEXT_FORMAT_ERROR = 404;// 策略Json格式不正确
    public static final int POLICY_ATTACHED_ERROR = 405; // 附加策略失败
    public static final int DELETE_POLICY_FAIL = 406; // 删除策略失败
    public static final int POLICY_NAME_INPUT_ERR = 407; // 策略名输入错误
    public static final int POLICY_CANNOT_BE_DELETED = 408; // 系统策略、模板策略不允许被删除

    /**
     * 用户组相关错误码
     **/
    public static final int GROUP_NOT_EXISTS = 501; // 用户组不存在
    public static final int TOO_MANY_GROUP = 502; // 用户组数量达到上限
    public static final int GROUP_EXISTED = 503; // 用户组已存在
    public static final int GROUP_HAD_USER = 504; // 用户组已经包含用户
    public static final int TOO_MANY_USER_IN_GROUP = 505; // 用户组可以包含的用户达到数量上限
    public static final int NO_SUCH_USER_IN_GROUP = 506; // 用户组不包含该用户
    public static final int GROUP_HAS_USERS_ERROR = 507;// 用户组下存在用户，无法删除
    public static final int GROUP_ATTACH_POLICY = 508;// 组附加了策略
    public static final int DELETE_GROUP_FAIL = 509; // 删除用户组失败
    public static final int GROUP_NAME_INPUT_ERR = 510; //组名输入错误

    /**
     * 账户相关错误码
     **/
    public static final int NO_SUCH_ACCOUNT = 601; // 账户不存在
    public static final int ACCOUNT_PASSWORD_ERROR = 602; // 账户密码错误
    public static final int ACCOUNT_EXISTED = 603; // 账户已存在
    public static final int INVALID_ACCOUNT = 604; // 无效的账户
    public static final int ACCOUNT_HAS_USERS_ERROR = 605;// 账户下存在用户，无法删除
    public static final int ACCOUNT_HAS_GROUPS_ERROR = 606;// 账户下存在用户组，无法删除
    public static final int TOO_MANY_ACCOUNT = 607; // 账户数量达到上限
    public static final int ACCOUNT_HAS_POLICIES_ERROR = 608;// 账户下存在策略，无法删除
    public static final int ACCOUNT_NAME_INPUT_ERROR = 609;// 账户输入的账户名错误
    public static final int INCONSISTENT_STORAGE_STRATEGY = 613; // 多站点间账户存储策略不一致
    public static final int ACCOUNT_VALIDITY_TIME_ERROR = 614; // 账户输入的密码有效期错误
    public static final int ACCOUNT_ID_EXISTED = 615; // 账户id已存在
    public static final int ACCOUNT_VALIDITY_GRADE_ERROR = 616;// 账户输入的密码有效期等级错误
    public static final int ACCOUNT_HAS_ROLES_ERROR = 617; // 账户下存在角色，无法删除

    /**
     * 授权相关错误码
     **/
    public static final int NO_SUCH_OBJ = 701; // 授权对象不存在
    public static final int LIST_LEN_DIFF = 702; // 输入list的长度不相等
    public static final int AUTH_EXISTS = 703; // 权限已存在
    public static final int AUTH_NOT_EXISTED = 704; // 权限不存在
    public static final int TOO_MANY_AUTH = 705; // 授权对象的策略数量达到上限
    public static final int DELETE_AUTH_FAIL = 706; // 删除授权失败
    public static final int POLICY_CANNOT_BE_ATTACHED = 707; // 模板策略不允许被附加

    /**
     * 输入相关错误码
     **/
    public static final int INVALID_LENGTH = 801; // 长度大于128

    /**
     * 同步MOSS操作失败
     **/
    public static final int MOSS_FAIL = 901; // 操作MOSS失败
    public static final int ACCOUNT_HAD_BUCKET = 902; // 账户下存在桶

    /**
     * 登录相关
     */
    public static final int LOGIN_INPUT_ERROR = 1001;
    public static final int TO_MANY_LOGIN = 1002;

    /**
     * 没有对象权限
     */
    public static final int NO_SUCH_OBJECT_READ_PERMISSION = 1101;
    public static final int NO_SUCH_OBJECT_WRITE_PERMISSION = 1102;
    /**
     * 对象不存在
     */
    public static final int NO_SUCH_KEY = 1103;


    /**
     * 指定的Location不存在或不合法
     */
    public static final int INVALID_LOCATION_CONSTRAINT = 2401;


    /**
     * 站点连接断开
     */
    public static final int SITE_LIKN_BROKEN = 2402;

    /**
     * 指定的站点不存在或不合法
     */
    public static final int INVALID_SITE_CONSTRAINT = 2403;

    /**
     * 站点数据同步，数据已修改
     */
    public static final int SYNC_DATA_MODIFIED = 2404;

    /**
     * 当前站点不满足条件
     */
    public static final int INSUFFICIENT_SITE_CONDITIONS = 2405;

    /**
     * 当源桶未开同步开关
     */
    public static final int SOURCE_BUCKET_NO_POLICY = 2406;

    /**
     * 当前站点不满足条件
     */
    public static final int INSUFFICIENT_BUCKET_CONDITIONS = 2407;

    public static final int NO_SUCH_CLUSTER_NAME = 2408;

    //检索池不存在
    public static final int NO_SUPPORT_METADATA_SEARCH = 2501;

    //桶策略相关错误码
    //桶策略不存在
    public static final int NO_SUCH_BUCKET_POLICY = 2601;

    //桶策略不合法
    public static final int INVALID_POLICY = 2602;

    public static final int INVALID_POLICY_PRINCIPAL = 2603;

    public static final int INVALID_POLICY_RESOURCE = 2604;

    public static final int INVALID_POLICY_ACTION = 2605;

    public static final int INVALID_POLICY_EFFECT = 2606;

    public static final int INVALID_POLICY_VERSION = 2607;

    public static final int INVALID_POLICY_STATEMENT = 2608;

    public static final int INVALID_POLICY_CONDITION = 2609;

    public static final int INVALID_POLICY_SID = 2610;

    /**
     * 存储策略错误码
     */
    public static final int NO_SUCH_STORAGE_STRATEGY = 1201;//没有创建策略

    public static final int INVALID_STORAGE_STRATEGY = 1301;

    /**
     * QoS性能限制相关错误码
     */
    public static final int NO_SUCH_LIMITSTRATEGY = 2703;

    public static final int INVALID_QOS_CONFIGURATION = 2704;

    public static final int NOTIFY_QOS_FAILED = 2705;  // 通知其它节点开启恢复QOS失败

    public static final int QOS_SET_ERROR = 2706;  // 通知其它节点开启恢复QOS失败

    /**
     * 策略管理错误码
     */
    public static final int NO_SUCH_COMPONENT_STRATEGY = 2901;
    public static final int DELETE_COMPONENT_STRATEGY_FAIL = 2905;
    public static final int STRATEGY_DESTINATION_ALREADY_EXISTS = 2907;
    public static final int STRATEGY_NAME_ALREADY_EXISTS = 2909;
    public static final int STRATEGY_DESTINATION_CONTAINS = 2910;
    public static final int NO_SUCH_COMPONENT_TASK = 2911;
    public static final int TASK_NAME_ALREADY_EXISTS = 2913;
    public static final int INVALID_COMPONENT_PARAM = 2921;
    public static final int TARGET_BUCKET_NOT_EXISTS = 2922;
    public static final int SOURCE_BUCKET_NOT_EXISTS = 2923;
    public static final int STRATEGY_ALREADY_EXISTS = 2924;
    public static final int IMAGE_FORMAT_NOT_SUPPORTED = 2925;
    public static final int IMAGE_FORMAT_NOT_EMPTY = 2926;
    public static final int NO_SUCH_PROCESS = 2927;
    public static final int UNSUPPORTED_SOURCE_FILE_FORMAT = 2928;
    public static final int STRATEGY_NOT_EXISTS = 2929;
    public static final int NO_SUCH_PROCESS_TYPE = 2930;
    public static final int NOT_BASE64_ENCODING = 2931;
    public static final int INVALID_INTERVAL_PARAM = 2932;
    public static final int INTERVAL_NOT_EMPTY = 2933;
    public static final int DELETE_SOURCE_CONFLICT = 2934;
    public static final int IMAGE_SIZE_TOO_LARGE = 2935;
    public static final int NOT_SUPPORTED_COMPONENT_PARAM = 2936;
    public static final int WATER_MARKER_IMAGE_NOT_FOUND = 2937;

    public static final int ROLE_NOT_EXISTS = 2707;
    public static final int ROLE_ALREADY_EXISTS = 2708;
    public static final int NO_AUTHORIZED = 2709;
    public static final int MALFORMED_ROLE_ARN = 2710;
    public static final int POLICY_ROLE_NOT_MATCH = 2711;
    public static final int INVALID_DURATION_SECONDS = 2712;
    public static final int NO_ASSUME_AUTHORIZED = 2713;
    public static final int MISSING_ROLE_NAME = 2714;
    public static final int POLICY_NOT_ATTACHABLE = 2715;
    public static final int POLICY_ALREADY_ATTACHED = 2716;
    public static final int POLICY_NOT_EXISTS_ROLE = 2717;
    public static final int DELETE_ROLE_POLICY_ERROR = 2718;
    public static final int MISSING_ROLE_SESSION_NAME = 2719;
    public static final int INVALID_ROLE_SESSION_NAME = 2720;
    public static final int INVALID_PATH = 2721;
    public static final int INVALID_ROLE_DESCRIPTION = 2723;
    public static final int INVALID_ROLE_NAME = 2722;
    public static final int INVALID_MAX_SESSION_DURATIONS = 2724;
    public static final int MISSING_POLICY_NAME = 2725;
    public static final int MALFORMED_POLICY_MRN = 2726;
    public static final int MISSING_POLICY_ARN = 2727;
    public static final int MISSING_ASSUME_ROLE_POLICY_DOCUMENT = 2728;
    public static final int INVALID_MARKER = 2729;
    public static final int INVALID_MAX_ITEMS = 2730;
    public static final int TOO_MANY_ENTITIES = 2731;
    public static final int MALFORMED_POLICY_DOCUMENT = 2732;
    public static final int INVALID_POLICY_NAME = 2733;
    public static final int MISSING_ROLE_DESCRIPTION = 2734;
    public static final int ROLE_SESSION_NAME_EXISTS = 2735;
    public static final int TOO_MANY_ROLES = 2736;
    public static final int ROLE_ATTACH_POLICY = 2737;
    /**
     * AWS IAM 错误码
     */
    public static final int AWS_IAM_NO_SUCH_ENTITY = 2801;


    /**
     * 数据追加写错误码
     */
    public static final int MISSING_POSITION = -2425;

    public static final int POSITION_NOT_EQUAL_TO_LENGTH = -2426;

    public static final int OBJECT_NOT_APPENDABLE = -2427;

    /**
     * 桶快照错误码
     */
    public static final int BUCKET_SNAPSHOT_NAME_INVALID = 2851;
    public static final int NO_SUCH_ROLLBACK_TARGET_SNAPSHOT = 2852;
    public static final int BUCKET_SNAPSHOT_SIZE_EXCEEDED = 2853;
    public static final int BUCKET_SNAPSHOT_NOT_ALLOWED = 2854;
    public static final int BUCKET_SNAPSHOT_LOCK_TIMEOUT = 2855;
    public static final int SNAPSHOT_MARK_OVERFLOW = 2856;
    public static final int NO_SUCH_BUCKET_SNAPSHOT = 2857;
    public static final int BUCKET_SNAPSHOT_EXIST = 2858;
    /**
     * 开启桶快照后，不允许执行的操作
     */
    public static final int OPERATION_CONFLICT = 2859;
    /**
     * 开启桶快照的条件不满足，如开启桶快照时开启元数据检索或数据同步
     */
    public static final int SNAPSHOT_CONFLICT = 2860;
    public static final int ROLLBACK_QUANTITY_EXCEEDS_LIMIT = 2861;
    public static final int CURRENTLY_RECOVERING_LATEST_DATA = 2862;
    public static final int CURRENTLY_IS_LATEST_DATA = 2863;
    public static final int CURRENTLY_IS_NOT_LATEST_DATA = 2864;
    public static final int ROLLBACK_SNAPSHOT_MARK_INVALID = 2865;

    /**
     * 设置文件配额错误码
     */
    public static final int DIR_NAME_NOT_FOUND = 3901;
    public static final int DIR_NAME_IS_SETTING = 3902;
    public static final int DIR_NAME_HAS_SET = 3903;
    public static final int QUOTA_TOO_MUCH = 3904;
    public static final int QUOTA_CREATE_FREQUENTLY = 3905;
    public static final int QUOTA_METHOD_CONFLICT = 3906;
    public static final int QUOTA_CAP_EXCEED_EXITS_LIMIT = 3907;
    public static final int QUOTA_FILE_EXCEED_EXITS_LIMIT = 3908;
    public static final int QUOTA_INFO_NOT_EXITS =  3909;
    public static final int QUOTA_INFO_LESS_THAN_USED =  3910;
    public static final int QUOTA_BUCKET_NOT_START_FS =  3911;
    public static final int QUOTA_DIR_NAME_IS_RENAMING =  3912;

    /**
     * nfs错误码
     */
    public static final int NO_SUCH_INSTANCE = 3101;

    public static final int INVALID_CACHE_WATER_MARK_CONFIG = 3201;
    public static final int NO_SUCH_CACHE_POOL = 3202;

    public static final int COMPRESS_SERVER_ERROR = 3301;

    public static final int INVALID_FS_STATUS = 3401;
}
