package com.macrosan.constants;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.vertx.core.eventbus.DeliveryOptions;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ServerConstants
 * <p>
 * 负责保存HTTP服务器相关常量
 *
 * @author liyixin
 * @date 2018/10/28
 */
public class ServerConstants {

    private ServerConstants() {
    }

    /**
     * verticle实例个数
     */
    public static final int PROC_NUM = Runtime.getRuntime().availableProcessors();

    /**
     * Event Bus 设置
     */
    public static final DeliveryOptions EVENT_BUS_OPTION = new DeliveryOptions().setSendTimeout(100 * 1000);

    /* -------------------------请求和响应头常量-------------------------*/
    /**
     * 请求头和响应头公用的常量
     */
    public static final String DATE = "Date";

    public static final String CONTENT_LENGTH = "Content-Length";

    public static final String CONTENT_TYPE = "Content-Type";

    public static final String HANG_UP_STATUS = "Hang-Up";

    public static final String MULTI_SYNC = "multi-sync";

    public static final String CONTENT_LANGUAGE = "Content-Language";

    public static final String CONTENT_MD5 = "Content-MD5";

    public static final String CONNECTION = "Connection";

    public static final String RANGE = "Range";

    public static final String VERSIONID = "versionId";
    public static final String PARTNUMBER = "partNumber";

    public static final String LAST_MODIFY = "Last-Modified";

    public static final String USER_AGENT = "User-Agent";

    public static final String SDK_USER_AGENT = "X-Amz-User-Agent";

    public static final String TRANSFER_ENCODING = "Transfer-Encoding";

    public static final String X_AMX_VERSION_ID = "x-amz-version-id";

    public static final String X_AMX_STORAGE_STRATEGY = "x-amz-storage-strategy";

    public static final String X_AMX_SOURCE_VERSION_ID = "x-amz-source-version-id";

    public static final String X_AMZ_COPY_SOURCE_RANGE = "x-amz-copy-source-range";

    public static final String X_AMX_DELETE_MARKER = "x-amz-delete-marker";

    public static final String REQUEST_ID = "request-id";

    public static final String VERSION_NUM = "version_num";

    public static final String SYNC_STAMP = "SyncStamp";

    public static final String LAST_STAMP = "LastStamp";

    public static final String IS_SYNCING = "syncing";

    public static final String EXPECT = "Expect";

    public static final String EXPECT_100_CONTINUE = "100-continue";

    /**
     * 请求头常量
     */
    public static final String HOST = "Host";

    public static final String CONTENT_ENCODING = "Content-Encoding";

    public static final String CONTENT_DISPOSITION = "Content-Disposition";

    public static final String CACHE_CONTROL = "Cache-Control";

    public static final String AUTHORIZATION = "Authorization";

    public static final String X_AMZ_DATE = "x-amz-date";

    public static final String X_AMZ_COPY_SOURCE = "x-amz-copy-source";

    public static final String X_AMZ_ACL = "x-amz-acl";

    public static final String X_AMZ_METADATA_DIRECTIVE = "x-amz-metadata-directive";

    public static final String X_AMZ_COPY_SOURCE_IF_MATCH = "x-amz-copy-source-if-match";

    public static final String X_AMZ_COPY_SOURCE_IF_NONE_MATCH = "x-amz-copy-source-if-none-match";

    public static final String X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE = "x-amz-copy-source-if-modified-since";

    public static final String X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE = "x-amz-copy-source-if-unmodified-since";

    public static final String X_AMZ_WORM_DATE_HEADER = "x-amz-object-lock-retain-until-date";

    public static final String X_AMZ_WORM_MODE_HEADER = "x-amz-object-lock-mode";

    public static final String X_AMZ_SERVER_SIDE_ENCRYPTION = "x-amz-server-side-encryption";

    public static final String POSITION = "position";

    public static final String X_AUTH_TOKEN = "x-auth-token";

    public static final String X_AMZ_STORE_MANAGEMENT_SERVER_HEADER = "x_amz_store_management_server_header";

    /**
     * 响应头常量
     */
    public static final String SERVER = "Server";

    public static final String ETAG = "ETag";

    public static final String X_AMZ_REQUEST_ID = "x-amz-request-id";

    public static final String ACCESS_CONTROL_ORIGIN = "Access-Control-Allow-Origin";
    public static final String ACCESS_CONTROL_METHOD = "Access-Control-Allow-Methods";
    public static final String ACCESS_CONTROL_ALLOW_HEADERS = "Access-Control-Allow-Headers";
    public static final String ACCESS_CONTROL_MAX_AGE = "Access-Control-Max-Age";
    public static final String ACCESS_CONTROL_VARY = "Vary";

    public static final String ACCESS_CONTROL_HEADERS = "Access-Control-Expose-Headers";

    public static final String NEXT_APPEND_POSITION = "x-amz-next-append-position";

    public static final String X_SUBJECT_TOKEN = "X-Subject-Token";

    public static final String X_OPENSTACK_REQUEST_ID = "X-OpenStack-Request-Id";

    public static final String X_ACCOUNT_CONTAINER_COUNT = "X-Account-Container-Count";

    public static final String X_CONTAINER_OBJECT_COUNT = "X-Container-Object-Count";

    public static final String X_CONTAINER_BYTES_USED = "X-Container-Bytes-Used";

    /**
     * 内部使用头常量
     */
    public static final String QUOTA = "quota";

    public static final String USER_ID = "_id";

    public static final String REQUEST_URI = "_uri";

    public static final String BUCKET_NAME = "_bucketname";

    public static final String OBJECT_NAME = "_objectname";

    public static final String CODEC = "_codec";

    public static final String UPLOAD_ID = "uploadId";

    public static final String USER_META = "x-amz-meta-";

    public static final String GRANT_ACL = "x-amz-grant-";

    public static final String MAX_PART = "max-parts";

    public static final String PART_NUMBER_MARKER = "part-number-marker";

    public static final String PREFIX = "prefix";

    public static final String MARKER = "marker";

    public static final String KEY_MARKER = "key-marker";

    public static final String VERSION_ID_MARKER = "version-id-marker";

    public static final String MAX_KEYS = "max-keys";

    public static final String DELIMITER = "delimiter";

    public static final String MAX_UPLOADS = "max-uploads";

    public static final String UPLOAD_ID_MARKER = "upload-id-marker";

    public static final String LOWER_CASE_CONTENT_LENGTH = "content-length";

    private static final String UPLOAD = "uploads";

    private static final String ACL = "acl";

    private static final String RETENTION = "retention";

    private static final String PART_NUMBER = "partNumber";

    private static final String VERSION_ID = "versionId";

    public static final String VERSIONS = "versions";

    private static final String META_DATA = "metadata";

    private static final String PERFORMANCEQUOTA = "performancequota";

    private static final String LIFECYCLE = "lifecycle";

    public static final String IF_MATCH = "x-amz-copy-source-if-";
    public static final String VERSIONING = "versioning";

    public static final String NEW_VERSION_ID = "new_version_id";

    /**
     * 含有该头部字段的httpRequest表示是经双活节点转发的，非客户端发送的原始请求。
     */
    public static final String CLUSTER_ALIVE_HEADER = "cluster-alive";

    /**
     * 双活修复分段上传时使用。格式为uploadId,partNum
     */
    public static final String GET_SINGLE_PART = "get-single-part";

    public static final String RECORD_ORIGIN_UPLOADID = "record_origin_uploadId";

    /**
     * 存放双活所需字段
     */
    public static final String CLUSTER_VALUE = "cluster-value";

    /**
     * chunked类型上传的对象的实际大小
     */
    public static final String ACTUAL_OBJECT_SIZE = "actual_object_size";
    /**
     * 源站点所使用的加密方式
     */
    public static final String ORIGIN_INDEX_CRYPTO = "origin_index_crypto";

    /**
     * 存放同步key值请求头
     */
    public static final String SYNC_RECORD_HEADER = "Sync-Key-Header";

    public static final String SYNC_V4_CONTENT_LENGTH = "Sync-V4-Content-Length";

    public static final String SYNC_COPY_PART_UPLOADID = "sync-copy-part-uploadId";

    public static final String SYNC_COPY_SOURCE = "sync-copy-source";

    /**
     * 请求头中包含此key-value的请求不进行多站点间的同步
     * 例如桶访问日志、桶清单等生成的文件以及对该文件做的处理
     */
    public static final String NO_SYNCHRONIZATION_KEY = "iff";
    public static final String NO_SYNCHRONIZATION_VALUE = "7e319d76ac718180151356061421ccfd";
    public static final String IFF_TAG = "iffTag";

    /**
     * 临时URL相关
     */
    public static final String SIGNATURE = "Signature";

    public static final String ACCESS_KEY_ID = "AccessKeyId";

    public static final String AWS_ACCESS_KEY_ID = "AWSAccessKeyId";

    public static final String EXPIRES = "Expires";

    public static final String URL_PREFIX = "Prefix";

    /**
     * ES相关
     */
    public static final String ES_META_DATE_SEARCH = "metaDateSearch";

    public static final String ES_OBJECT_NAME = "objName";

    public static final String ES_USER_META = "userMeta";

    public static final String ES_MAKER = "marker";

    public static final String ES_NUMBER = "number";

    public static final String ES_MAX_NUMBER = "maxNumber";

    public static final String ES_LAST_MODIFIED = "lastModified";

    public static final String DELETE = "delete";

    public static final String META_DATA_SEARCH = "metaDataSearch";

    private static final String MULTI_REGION_INFO = "multiRegionInfo";

    private static final String LOCATION = "location";

    private static final String POLICY = "policy";

    /**
     * listObjectV2相关
     */

    public static final String LIST_TYPE = "list-type";

    public static final String START_AFTER = "start-after";

    public static final String CONTINUATION_TOKEN = "continuation-token";

    public static final String FETCH_OWNER = "fetch-owner";

    public static final String NEXT_CONTINUATION_TOKEN = "next-continuation-token";

    /**
     * 管理容量相关
     */
    public static final String MGT_CAP_SYS = "syscapinfo";
    public static final String MGT_CAP_ACCOUNT = "accountusedcapacity";
    public static final String MGT_CAP_ACCOUNT_NAME = "account-name";

    /**
     * 性能配相关
     */
    public static final String PERFORMANCE_QUOTA_ACCOUNT = "performancequota";
    public static final String PERFORMANCE_QUOTA_ACCOUNT_NAME = "account-name";
    public static final String PERFORMANCE_QUOTA_TYPE = "type";
    public static final String PERFORMANCE_QUOTA_VALUE = "quota";

    /**
     * QoS性能限制相关
     */
    public static final String QOS_LIMIT_STRATEGY = "limitStrategy";
    public static final String QOS_LIMIT_STRATEGY_DATA = "LIMIT";         // 业务优先
    public static final String QOS_LIMIT_STRATEGY_RECOVER = "NO_LIMIT";   // 数据恢复优先
    public static final String QOS_LIMIT_STRATEGY_ADAPT = "ADAPT";        // 自适应策略
    public static final String QOS_START_TIME = "startTime";
    public static final String QOS_END_TIME = "endTime";
    public static final String QOS_RECOVER_KEY = "recover*";
    public static final String QOS_RECORD_NUM = "recordNum";
    public static final String QOS_RSOCKET_PREFIX = "record_";

    /**
     * 统计相关
     */
    public static final String MGT_STAT_START_TIME = "startTime";
    public static final String MGT_STAT_END_TIME = "endTime";
    public static final String MGT_STAT_REQ_TYPE = "requestType";
    public static final String MGT_STAT_REQ_TYPE_READ = "read";
    public static final String MGT_STAT_REQ_TYPE_WRITE = "write";
    public static final String MGT_STAT_REQ_TYPE_TOTAL = "total";
    public static final String MGT_STAT_REQ_TYPE_GET = "get";
    public static final String MGT_STAT_REQ_TYPE_PUT = "put";
    public static final String MGT_STAT_REQ_TYPE_POST = "post";
    public static final String MGT_STAT_REQ_TYPE_DELETE = "delete";
    public static final String MGT_STAT_REQ_TYPE_HEAD = "head";
    public static final String MGT_STAT_START_STAMP = "startStamp";
    public static final String MGT_STAT_END_STAMP = "endStamp";
    public static final String MGT_STAT_REQUEST_TYPE = "requestsType";
    public static final String MGT_STAT_BUCKET = "bucket";
    public static final String MGT_STAT_ACCOUNT = "accountId";
    public static final String MGT_STAT_CURRENT_STAMP = "currentStamp";
    /**
     * 需要过滤的请求头集合
     */
    public static final IntList EXCLUDE_HEADER_LIST = new IntArrayList().with(
            HOST.hashCode(),
            REQUEST_URI.hashCode(),
            AUTHORIZATION.hashCode(),
            CONNECTION.hashCode(),
            CODEC.hashCode()
    );

    /**
     * 不需要鉴权的资源路径
     */
    public static final IntList EXCLUDE_AUTH_RESOURCE = new IntArrayList().with(
            "GET/?locationInfo".hashCode()
    );

    /**
     * 需要进行限流的STS资源路径
     */
    public static final IntList LIMIT_STS_RESOURCE = new IntArrayList().with(
            "POST/?Action=AssumeRole".hashCode()
    );

    /**
     * 路由需要包含的查询参数
     * 注：现在该List自动初始化
     */
    public static final IntArrayList INCLUDE_PARAM_LIST = new IntArrayList();

    /**
     * 鉴权需要包含的查询参数
     */
    public static final IntList SIG_INCLUDE_PARAM_LIST = new IntArrayList().with(
            UPLOAD_ID.hashCode(),
            UPLOAD.hashCode(),
            ACL.hashCode(),
            "append".hashCode(),
            "position".hashCode(),
            RETENTION.hashCode(),
            PART_NUMBER.hashCode(),
            VERSION_ID.hashCode(),
            VERSIONING.hashCode(),
            VERSIONS.hashCode(),
            LIFECYCLE.hashCode(),
            DELETE.hashCode(),
            PERFORMANCEQUOTA.hashCode(),
            META_DATA_SEARCH.hashCode(),
            MULTI_REGION_INFO.hashCode(),
            LOCATION.hashCode(),
            "putMetadataAnalysisSwitch".hashCode(),
            "datasynchronization".hashCode(),
            POLICY.hashCode(),
            "inventory".hashCode(),
            "object-lock".hashCode(),
            "objects-quota".hashCode(),
            "createaccount".hashCode(),
            "accountcapacityquota".hashCode(),
            "accountobjectsquota".hashCode(),
            "accountinfo".hashCode(),
            "resetaccountpassword".hashCode(),
            "listaccountaccesskeys".hashCode(),
            "deleteaccount".hashCode(),
            "updateaccount".hashCode(),
            "updateaccountname".hashCode(),
            "listaccounts".hashCode(),
            "resetaccountpassword".hashCode(),
            "createaccountaccesskey".hashCode(),
            "createadminaccesskey".hashCode(),
            "deleteaccountaccesskey".hashCode(),
            "deleteadminaccesskey".hashCode(),
            "createuser".hashCode(),
            "deleteuser".hashCode(),
            "updateuser".hashCode(),
            "updateusername".hashCode(),
            "updateuserpasswd".hashCode(),
            "listusers".hashCode(),
            "getuser".hashCode(),
            "createaccesskey".hashCode(),
            "deleteaccesskey".hashCode(),
            "listaccesskeys".hashCode(),
            "creategroup".hashCode(),
            "deletegroup".hashCode(),
            "updategroup".hashCode(),
            "listgroups".hashCode(),
            "getgroup".hashCode(),
            "adduserstogroups".hashCode(),
            "removeuserfromgroup".hashCode(),
            "listusersforgroup".hashCode(),
            "listgroupsforuser".hashCode(),
            "createpolicy".hashCode(),
            "deletepolicy".hashCode(),
            "updatepolicy".hashCode(),
            "listpolicies".hashCode(),
            "getpolicy".hashCode(),
            "listentitiesforpolicy".hashCode(),
            "attachuserpolicy".hashCode(),
            "attachentitiespolicy".hashCode(),
            "detachuserpolicy".hashCode(),
            "listuserpolicies".hashCode(),
            "attachgrouppolicy".hashCode(),
            "detachgrouppolicy".hashCode(),
            "listgrouppolicies".hashCode(),
            "listauths".hashCode(),
            "logging".hashCode(),
            "notification".hashCode(),
            "trash".hashCode(),
            "objectCheck".hashCode(),
            "encryption".hashCode(),
            "componentStrategy".hashCode(),
            "snapshot".hashCode(),
            "fsperformancequota".hashCode(),
            "fsaddressperformancequota".hashCode(),
            "PutFsDirQuotaInfo".hashCode(),
            "PutFsUserQuotaInfo".hashCode(),
            "PutFsGroupQuotaInfo".hashCode(),
            "DelFsQuotaInfo".hashCode(),
            "FsQuotaUsedInfo".hashCode(),
            "FsQuotaConfig".hashCode(),
            "FsQuotaConfigs".hashCode(),
            "tagging".hashCode(),
            "cors".hashCode(),
            "referer".hashCode(),
            "clear".hashCode(),

            //添加getObject需要校验的请求头
            "response-content-language".hashCode(),
            "response-expires".hashCode(),
            "response-cache-control".hashCode(),
            "response-content-disposition".hashCode(),
            "response-content-encoding".hashCode(),
            "response-content-type".hashCode(),
            "capinfo".hashCode(),
            "size".hashCode(),
            "sync".hashCode(),
            "clusterstatus".hashCode(),
            "clustername".hashCode(),
            "indexHisSync".hashCode(),
            "TrafficStatisticsList".hashCode(),
            "getAWSSigns".hashCode(),
            "setAWSSign".hashCode(),
            "deleteAWSSign".hashCode(),
            "getFsAcl".hashCode()
    );


    /**
     * 存放不需要进行encode 的请求parameter中的参数的list
     * <p>
     * 添加getObject 请求指定覆盖响应标头值
     * https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/RESTAuthentication.html
     * 签名时，请勿对这些值进行编码；但是进行请求时，您必须对这些参数值进行编码。
     */
    public static final IntList NO_ENCODE_PARAM_LIST = new IntArrayList().with(
            "response-content-language".hashCode(),
            "response-expires".hashCode(),
            "response-cache-control".hashCode(),
            "response-content-disposition".hashCode(),
            "response-content-encoding".hashCode(),
            "response-content-type".hashCode()
    );

    /**
     * 上传时可以携带的六个特殊header
     */
    public static final IntList SPECIAL_HEADER = new IntArrayList().with(
            CACHE_CONTROL.toLowerCase().hashCode(),
            CONTENT_DISPOSITION.toLowerCase().hashCode(),
            CONTENT_ENCODING.toLowerCase().hashCode(),
            CONTENT_TYPE.toLowerCase().hashCode(),
            EXPIRES.toLowerCase().hashCode(),
            CONTENT_LANGUAGE.toLowerCase().hashCode()
    );

    /**
     * 管理员admin专用方法
     */
    public static final List<String> MANAGER_ACTION_TYPE_LIST = new FastList<String>(4).with(
            "getSysCapacityInfo".toUpperCase(),
            "getAccountUsedCap".toUpperCase(),
            "getPerformanceQuota".toUpperCase(),
            "putPerformanceQuota".toUpperCase(),
            "getClusterStatus".toUpperCase()
    );

    public static final List<String> HOST_LIST = new ArrayList<>();
    public static final List<String> ONLY_HOST_LIST = new ArrayList<>();

    /**
     * ParamMap 中的key
     */
    public static final String METHOD = "method";

    public static final String BODY = "body";

    public static final String REQUESTID = "requestid";

    public static final String ID = "id";

    public static final String USERNAME = "userName";

    public static final String SOURCEIP = "sourceIp";

    public static final String MOSSSOURCEIP = "moss:sourceIp";

    /* -------------------------路由配置文件路径----------------- */
    public static final String MANAGE_ROUTE = File.separator + "route" + File.separator + "manage";

    public static final String DATA_ROUTE = File.separator + "route" + File.separator + "data";

    public static final String ERROR_MAP = File.separator + "errormsg" + File.separator + "errno";

    public static final String ACTION_MAP = File.separator + "policy" + File.separator + "action";

    /* -------------------------请求参数相关 -------------------*/
    /**
     * URL参数
     */
    public static final String URL_PARAM_ACL = "acl";

    public static final String URL_PARAM_UPLOADS = "uploads";

    public static final String URL_PARAM_UPLOAD_ID = "upload_id";

    public static final String URL_PARAM_UPLOADID = "uploadId";

    public static final String URL_PARM_MAX_PARTS = "max-parts";

    public static final String URL_PARM_PART_NUMBER_MARKER = "part-number-marker";

    public static final String URL_PARAM_PART_NUMBER = "partNumber";

    /* ------------------------xml相关--------------------------------- */
    /**
     * xml类放置的位置
     */
    public static final String XML_PACKAGE_NAME = "com.macrosan.message.xmlmsg";

    /**
     * 默认XML错误码
     */
    public static final String INTERNAL_ERROR_CODE = "InternalError";

    /**
     * 默认XML错误信息
     */
    public static final String INTERNAL_ERROR_MSG = "The system encountered an internal error, please try again.";


    /**
     * -------------------------Event Bus地址 ----------------------------
     */
    public static final String MANAGE_STREAM_ADDRESS = "ManageStreamController";
    public static final String IAM_MANAGE_STREAM_ADDRESS = "IamManageStreamController";
    public static final String STS_MANAGE_STREAM_ADDRESS = "StsManageStreamController";
    /**
     * -------------------------Authorize相关------------------------------
     */
    public static final String AUTH_TYPE_V2 = "AWS";

    public static final String AUTH_TYPE_V4 = "AWS4-HMAC-SHA256";

//    public static final String AUTH_TYPE = "authType";

    public static final String AUTH_HEADER = "x-amz-";

    public static final String LINE_BREAKER = "\n";

    public static final char COLON = ':';

    public static final char EQUAL = '=';

    public static final String SLASH = "/";

    public static final String ALGORITHM_HMACSHA1 = "HmacSHA1";

    public static final String SECRET_KEY = "secret_key";

    public static final String ACCESS_KEY = "ak";

    public static final String MRN = "mrn";

    public static final String MOSS = "moss";

    public static final String IAM = "iam";

    public static final String USERPREFIX = "user/";

    public static final String ROOT = "root";


    /**
     * -------------------------------------HTTP错误码-------------------------------------
     */
    public static final int SUCCESS = 200;

    public static final int BAD_REQUEST_REQUEST = 400;

    public static final int CONFLICT = 409;

    public static final int FORBIDDEN = 403;

    public static final int INTERNAL_SERVER_ERROR = 500;

    public static final int NOT_FOUND = 404;

    public static final int NOT_ALLOWED = 405;

    public static final int DEL_SUCCESS = 204;

    public static final int CREATED = 201;

    public static final int UNAUTHORIZED = 401;

    /**
     * -------------------------------空Bytes------------------------------------------
     */
    public static final byte[] DEFAULT_DATA = new byte[0];

    public static final ByteBuf DEFAULT_BUF_DATA = Unpooled.EMPTY_BUFFER;

    public static final Map<String, String> EMPTY_MAP = new HashMap<>(0);


    /**
     * -------------------------------IAM 相关------------------------------------------
     */

    public static final String AUTH_OBJECT_TYPE_USER = "1"; // 授权对象的类型为用户
    public static final String AUTH_OBJECT_TYPE_GROUP = "2"; // 授权对象的类型为用户组
    public static final String AUTH_OBJECT_TYPE_ROLE = "3"; // 授权对象的类型为用户组
    public static final String XML_RESPONSE = "application/xml";


    /**
     * ---------------------------Component Strategy 相关----------------------------
     */
    public static final String COMPONENT_STRATEGY_REDIS_PREFIX = "component_strategy_";
    public static final String COMPONENT_STRATEGY_NAME = "strategyName";
    public static final String COMPONENT_STRATEGY_TYPE = "image-process";
    public static final String COMPONENT_STRATEGY_DESTINATION = "destination";
    public static final String COMPONENT_STRATEGY_DELETE_SOURCE = "deleteSource";
    public static final String COMPONENT_STRATEGY_COPY_USER_META_DATA = "copyUserMetaData";

    /**
     * ---------------------------Component_Task 相关------------------------------
     */
    public static final String COMPONENT_TASK_NAME = "taskName";
    public static final String COMPONENT_TASK_BUCKET = "taskBucket";
    public static final String COMPONENT_TASK_PREFIX = "objectPrefix";
    public static final String COMPONENT_TASK_REDIS_PREFIX = "component_task_";
    public static final String COMPONENT_TASK_TIME = "startTime";
    public static final String COMPONENT_TASK_STRATEGY_NAME = "strategyName";
    public static final String COMPONENT_TASK_TYPE = "image-process";

    public static final String COMPONENT_TASK_DELETE_SET = "delete_task_set";
    public static final String COMPONENT_RECORD_BUCKET_SET = "record_bucket_set";

    public static final String COMPONENT_RECORD_INNER_MARKER = "inner";
    /**
     * ----------------------------Component_Error_Record 相关----------------------------
     */

    public static final String TASK_ERROR_RECORD_PREFIX = "task_error_";
    public static final String OBJECT_ERROR_RECORD_PREFIX = "object_error_";
    public static final int MAXIMUM_PIXEL_ERROR_CODE = 10000;

    /**
     * ----------------------------桶快照 相关----------------------------
     */
    public static final String BUCKET_SNAPSHOT_PREFIX = "bucket_snapshot_";
    public static final String SNAPSHOT_SWITCH = "snapshot-switch";
    public static final String CURRENT_SNAPSHOT_MARK = "current_snapshot_mark";
    public static final String SNAPSHOT_LINK = "snapshot_link";
    public static final String SNAPSHOT_DISCARD_LIST_PREFIX = "snapshot_discard_list_";
    public static final String SNAPSHOT_MERGE_TASK_PREFIX = "snapshot_merge_task_";
    public static final String MERGE_PROGRESS = "merge_progress_";
    public static final String CLEAN_UP_PROGRESS = "clean_up_progress_";
    public static final String DATA_MERGE_MAPPING = "data_merge_mapping";

    /**
     * ----------------------------存储池重平衡 相关----------------------------
     */
    public static final String REMOVE_NODE = "remove_node";
    public static final String REMOVE_DISK = "remove_disk";
    public static final String PREPARE_ADD_NODE_PREFIX = "prepare_add_node_set_";
    public static final String CLASH_ADD_NODE_PREFIX = "clash_add_node_set_";
    public static final String TO_NEW_ADD_NODE_PREFIX = "to_new_add_node_set_";
    public static final String IN_SRC_ADD_NODE_PREFIX = "in_src_add_node_set_";
}
