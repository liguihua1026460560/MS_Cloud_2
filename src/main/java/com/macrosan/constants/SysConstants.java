package com.macrosan.constants;

import com.macrosan.doubleActive.archive.ArchiveAnalyzer;
import com.macrosan.utils.regex.Pattern;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.*;

/**
 * ServerConstants
 * <p>
 * 负责保存系统相关常量
 *
 * @author liyixin
 * @date 2018/12/4
 */
public class SysConstants {

    private SysConstants() {
    }

    public static final String ROCKS_OBJ_META_DELETE_MARKER = "~";
    public static final String ROCKS_PART_PREFIX = "!";
    public static final String ROCKS_PART_META_PREFIX = "@";
    public static final String ROCKS_FILE_META_PREFIX = "#";
    public static final String ROCKS_BUCKET_META_PREFIX = "$";
    public static final String ROCKS_STATIC_PREFIX = "&";
    public static final String ROCKS_VERSION_PREFIX = "*";
    public static final String ROCKS_LIFE_CYCLE_PREFIX = "+";
    public static final String ROCKS_FILE_SYSTEM_PREFIX = ".";
    public static final String ROCKS_SPECIAL_KEY = "%";
    public static final String ROCKS_LATEST_KEY = "-";
    public static final String ROCKS_SMALLEST_KEY = "0";
    public static final String ROCKS_UNSYNCHRONIZED_KEY = "|";
    public static final String ROCKS_SINGLESYNC_KEY = ",";
    public static final String ROCKS_DEDUPLICATE_KEY = "^";
    public static final String ROCKS_COMPONENT_VIDEO_KEY = ":";
    public static final String ROCKS_COMPONENT_IMAGE_KEY = "!";
    public static final String ROCKS_INODE_PREFIX = "(";
    public static final String ROCKS_CHUNK_FILE_KEY = ")";
    public static final String ROCKS_ES_KEY = "'";
    public static final String ROCKS_COOKIE_KEY = "<";
    public static final String ROCKS_STS_TOKEN_KEY = "=";

    public static final String ROCKS_AGGREGATION_META_PREFIX = "?";
    public static final String ROCKS_AGGREGATION_RATE_PREFIX = "%";
    public static final String ROCKS_AGGREGATION_UNDO_LOG_PREFIX = ">";

    public static final String ROCKS_CACHE_ORDERED_KEY = "{";
    public static final String ROCKS_CACHE_ACCESS_KEY = "}";

    //文件数据冷热分层key prefix
    public static final String ROCKS_CACHE_BACK_STORE_KEY = "`";

    /*************socket通信端口***************/
    public static final int SOCKET_PORT1 = 11111;
    public static final int SOCKET_PORT2 = 11112;
    public static final int SOCKET_PORT3 = 11113;
    public static final int SOCKET_PORT4 = 11114;

    //5,6用于发送IAM管理服务器端口
    public static final int SOCKET_PORT7 = 11117;
    public static final int SOCKET_PORT8 = 11118;
    public static final int MONGODB_PORT1 = 27017;
    public static final int MONGODB_PORT2 = 27018;

    /*************redis 端口 和 数据库index******/
    public static final int REDIS_PORT = 6379;
    public static final int REDIS_ROCK_INDEX = 0;
    public static final int REDIS_SYSINFO_INDEX = 2;
    public static final int REDIS_USERINFO_INDEX = 3;
    public static final int REDIS_TASKINFO_INDEX = 4;
    public static final int REDIS_FS_QUOTA_INFO_INDEX = 5;
    public static final int REDIS_MAPINFO_INDEX = 6;
    public static final int REDIS_BUCKETINFO_INDEX = 7;
    public static final int REDIS_NODEINFO_INDEX = 8;
    public static final int REDIS_UPLOAD_INDEX = 9;
    public static final int REDIS_LUNINFO_INDEX = 10;
    public static final int REDIS_PERCENT_INDEX = 11;
    public static final int REDIS_MIGING_V_INDEX = 12;
    public static final int REDIS_RATE_LIMIT_INDEX = 13;
    public static final int REDIS_SNAPSHOT_INDEX = 14;
    public static final int REDIS_POOL_INDEX = 15;

    /**
     * IAM相关
     **/
    public static final int REDIS_IAM_INDEX = 0;
    public static final int REDIS_ACTION_INDEX = 1;
    public static final int REDIS_TOKEN_INDEX = 2;
    public static final int REDIS_COMPONENT_ERROR_INDEX = 3;
    public static final String DEFAULT_ACCOUNTID = "";
    public static final String IAM_COFIG_PATH = "/moss/iam/iam.conf";
    public static final String IAM_COFIG_SERVER_IP = "server_ip";
    public static final String DEFAULT_SERVER_IP = "0";

    /*************配置文件路径***************/
    public static final String PUBLIC_CONF_FILE;
    public static final String PUBLIC_PEER_CONF_FILE;

    static {
        if (System.getProperty("com.macrosan.takeOver") != null) {
            PUBLIC_CONF_FILE = "/moss/ms_moss/public_peer.conf";
            PUBLIC_PEER_CONF_FILE = "/moss/ms_moss/public.conf";
        } else {
            PUBLIC_CONF_FILE = "/moss/ms_moss/public.conf";
            PUBLIC_PEER_CONF_FILE = "/moss/ms_moss/public_peer.conf";
        }
    }

    public static final String MASTER_CONF_FILE = "/moss/ms_moss/master.conf";
    public static final String MOUNT_CONF_FILE = "/moss/ms_moss/mount.conf";
    public static final String MONGODB_DIR = "/moss/ms_moss/mongodb/";
    public static final String MONGODB_CONF = "/moss/ms_moss/mongodb.conf";
    public static final String MONGODB_CONF_TAKEOVER = "/moss/ms_moss/mongodb_takeover.conf";
    public static final String PRIVATE_PEM = "/moss/ms_moss/private.pem";
    public static final String CERT_CRT = "/moss/ms_moss/cert.crt";
    public static final String RABBITMQ_ENV_CONF_FILE = "/etc/rabbitmq/rabbitmq-env.conf";

    public static final FastList<String> CONF_PATH_LIST = FastList.newListWith(PUBLIC_CONF_FILE,
            PUBLIC_PEER_CONF_FILE,
            MASTER_CONF_FILE,
            MOUNT_CONF_FILE,
            MONGODB_CONF,
            MONGODB_CONF_TAKEOVER,
            IAM_COFIG_PATH,
            RABBITMQ_ENV_CONF_FILE);
    /*************通讯网口1、2******************/
    public static final String BUSINESS_ETH1 = "business_eth1";
    public static final String BUSINESS_ETH2 = "business_eth2";
    public static final String BUSINESS_ETH3 = "business_eth3";
    public static final String BUSINESS_ETH1_IPV6 = "business_eth1_ipv6";
    public static final String BUSINESS_ETH2_IPV6 = "business_eth2_ipv6";
    public static final String BUSINESS_ETH3_IPV6 = "business_eth3_ipv6";
    public static String HEART_ETH1 = "heartbeat_eth1";
    public static String HEART_ETH2 = "heartbeat_eth2";
    public static String HEART_IP = "heart_ip";
    public static String SYNC_IP = "syncIp";
    public static String OUTER_MANAGER_IP = "outer_manager_ip";

    /*************跟管理系统通信的管理口******************/
    public static final String MGT_ETH = "mgt_eth";

    /*************master_vip1、2******************/
    public static final String MASTER_VIP1 = "master_vip1";
    public static final String MASTER_VIP2 = "master_vip2";

    public static final String VM_IP_ETH4 = "vm_ip_eth4";
    public static final String VM_IP_ETH5 = "vm_ip_eth5";
    public static final String OPP_VM_IP_ETH4 = "opp_vm_ip_eth4";
    public static final String OPP_VM_IP_ETH5 = "opp_vm_ip_eth5";

    /*************对端节点uuid******************/
    public static final String OPP_NODE_UUID = "opp_vm_uuid";
    public static final String NODE_UUID = "vm_uuid";
    //san 设备的uuid
    public static final String SAN_DEVICE_UUID = "device_uuid";
    //所属的sp
    public static final String SAN_DEVICE_SP = "sp";

    public static final String V_NODE_FILE = "/moss/ms_moss/vnode";
    public static final String NODE_STATE = "state";
    public static final String VM_UUID = "vm_uuid";
    public static final String NODE_INIT_STATE = "map_state";
    public static final String DNS_IP_KEY = "dns_ip";
    public static final String HEART_IP_KEY = "heart_vip";
    public static final String OPP_HEART_IP = "opp_heart_ip";
    public static final String HASH_NR_KEY = "hash_nr";
    public static final String IP_LIST = "ip_list";
    public static final String CONFIG_IP_LIST = "config_ip_list";
    public static final String S_NUM = "s_num";
    public static final String BUCKET_MAX_NR = "bucket_max_nr";
    public static final String USER_MAX_NR = "user_max_nr";
    public static final String REMOVE_RETAIN_SIZE = "remove_retain_size";
    public static final String V_NUM = "v_num";
    public static final String KEEPALIVED_ETH = "keepalived_eth";
    public static final String LUN_IN_S = "lun_in_s";
    public static final String G_IN_S = "g_in_s";
    public static final String LUN_IN_G = "lun_in_g";
    public static final String V_NODE_ID = "v_node_id";
    public static final String LOCAL_CONF = "local";
    public static final String REMOTE_CONF = "remote";
    public static String NODE_SERVER_STATE = "server_state";
    public static final String LOCAL_IP_ADDRESS = "127.0.0.1";

    /*************上限定义***************/
    public static final int MAX_TASK_IN_REMOVE = 10;
    //put对象最大容量5G
    public static final long MAX_PUT_SIZE = (long) 5 * 1024 * 1024 * 1024;
    //整个分片允许的最大容量5T
    public static final long MAX_UPLOAD_TOTAL_SIZE = (long) 5 * 1024 * 1024 * 1024 * 1024;

    //bucket最大未完成的分片任务数量
    public static final int MAX_UPLOAD_IN_BUCKET = 1000;
    //一个分片最大可以的part数目
    public static int MAX_UPLOAD_PART_NUM = 10000;

    public static final long GMT_TO_LOCAL = 8 * 60 * 60 * 1000L;

    public static final long MAX_COPY_LIMIT_SIZE = (long) 1024 * 1024;

    /*************socket报文消息类型******************/
    public static final String MSG_TYPE_CREATE_BUCKET = "create_bucket";
    public static final String MSG_TYPE_DEL_BUCKET = "del_bucket";
    public static final String MSG_TYPE_LS_BUCKET = "ls_bucket";
    public static final String MSG_TYPE_UPLOAD_OBJECT = "upload_object";
    public static final String MSG_TYPE_INIT_MUL_UPLOAD = "init_mul_upload";
    public static final String MSG_TYPE_UPLOAD_PART = "upload_part";
    public static final String MSG_TYPE_END_MUL_UPLOAD = "end_mul_upload";
    public static final String MSG_TYPE_LIST_PARTS = "list_parts";
    public static final String MSG_TYPE_DOWNLOAD_OBJECT = "download_object";
    public static final String MSG_TYPE_DEL_OBJECT = "del_object";
    public static final String MSG_TYPE_DEL_BUCKET_OBJ_DBFIEL = "del_bucket_obj_dbfile";
    public static final String MSG_TYPE_GET_OBJECTINFO = "get_objectInfo";
    public static final String MSG_TYPE_GET_OBJECTACL = "get_object_acl";
    public static final String MSG_TYPE_UPDATE_OBJECT_ACL = "update_object_acl";
    public static final String MSG_TYPE_ABORT_UPLOAD = "abort_mul_upload";
    public static final String MSG_TYPE_LS_UPLOADS = "ls_mul_uploads";
    public static final String MSG_TYPE_DEL_ALL_OBJECT = "del_all_object";
    public static final String MSG_TYPE_GET_ACCOUNT_USED_CAP = "get_account_used_cap";
    public static final String MSG_TYPE_GET_TRAFFIC_STATISTICS = "get_traffic_statistics";
    public static final String MSG_TYPE_GET_OBJECT_STATISTICS = "get_object_statistics";
    public static final String MSG_TYPE_COPY_OBJECT = "copy_object";
    public static final String MSG_TYPE_SYNC_REDIS = "sync_redis";


    //iam
    public static final String MSG_TYPE_CREATE_ACCOUNT = "create_account";
    public static final String MSG_TYPE_DELETE_ACCOUNT = "delete_account";
    public static final String MSG_TYPE_UPDATE_ACCOUNT_NAME = "update_account_name";
    public static final String MSG_TYPE_UPDATE_ACCOUNT_PASSWORD = "update_account_password";
    public static final String MSG_TYPE_UPDATE_ACCOUNT = "update_account";
    public static final String MSG_TYPE_UPDATE_ACCOUNT_CAPACITY = "update_account_capacity";
    public static final String MSG_TYPE_LIST_ACCOUNTS = "list_accounts";
    public static final String MSG_TYPE_GET_ACCOUNT_INFO = "get_account_info";

    public static final String MSG_TYPE_CREATE_ACCESSKEY_ACCOUNT = "create_accessKey_account";
    public static final String MSG_TYPE_CREATE_ADMIN_ACCESSKEY = "create_admin_accessKey";
    public static final String MSG_TYPE_DELETE_ACCESSKEY_ACCOUNT = "delete_accessKey_account";
    public static final String MSG_TYPE_DELETE_ADMIN_ACCESSKEY = "delete_admin_accessKey";

    public static final String MSG_TYPE_CREATE_USER = "create_user";
    public static final String MSG_TYPE_DELETE_USER = "delete_user";
    public static final String MSG_TYPE_UPDATE_USER = "update_user";
    public static final String MSG_TYPE_UPDATE_USER_NAME = "update_user_name";
    public static final String MSG_TYPE_LIST_USERS = "list_users";
    public static final String MSG_TYPE_GET_USER = "get_user";
    public static final String MSG_TYPE_UPDATE_USER_PASSWORD = "update_user_password";

    public static final String MSG_TYPE_CREATE_ACCESSKEY_USER = "create_accessKey_user";
    public static final String MSG_TYPE_DELETE_ACCESSKEY_USER = "delete_accessKey_user";
    public static final String MSG_TYPE_CREATE_ACCESSKEY = "create_accessKey";
    public static final String MSG_TYPE_DELETE_ACCESSKEY = "delete_accessKey";
    public static final String MSG_TYPE_LIST_ACCESSKEYS = "list_accessKeys";
    public static final String MSG_TYPE_LIST_ACCESSKEYS_ACCOUNT = "list_accessKeys_account";

    public static final String MSG_TYPE_CREATE_GROUP = "create_group";
    public static final String MSG_TYPE_DELETE_GROUP = "delete_group";
    public static final String MSG_TYPE_UPDATE_GROUP = "update_group";
    public static final String MSG_TYPE_LIST_GROUPS = "list_groups";
    public static final String MSG_TYPE_GET_GROUP = "get_group";
    public static final String MSG_TYPE_ADD_USERS_TO_GROUPS = "add_users_to_groups";
    public static final String MSG_TYPE_REMOVE_USER_FROM_GROUP = "remove_user_from_group";
    public static final String MSG_TYPE_LIST_USERS_FOR_GROUP = "list_users_for_group";
    public static final String MSG_TYPE_LIST_GROUPS_FOR_USER = "list_groups_for_user";

    public static final String MSG_TYPE_CREATE_ROLE = "create_role";
    public static final String MSG_TYPE_DELETE_ROLE = "delete_role";
    public static final String MSG_TYPE_LIST_ROLE = "list_role";
    public static final String MSG_TYPE_GET_ROLE = "get_role";
    public static final String MSG_TYPE_UPDATE_ROLE = "update_role";
    public static final String MSG_TYPE_UPDATE_ROLE_DESCRIPTION = "update_role_description";
    public static final String MSG_TYPE_ATTACH_ROLE_POLICY = "attach_role_policy";
    public static final String MSG_TYPE_PUT_ROLE_POLICY = "put_role_policy";
    public static final String MSG_TYPE_GET_ROLE_POLICY = "get_role_policy";
    public static final String MSG_TYPE_DELETE_ROLE_POLICY = "delete_role_policy";
    public static final String MSG_TYPE_LIST_ROLE_POLICY = "list_role_policy";
    public static final String MSG_TYPE_UPDATE_ASSUMEROLE_POLICY = "update_assume_role_policy";
    public static final String MSG_TYPE_LIST_ATTACH_ROLE_POLICY = "list_attach_role_policy";
    public static final String MSG_TYPE_DETACH_ROLE_POLICY = "detach_role_policy";
    public static final String MSG_TYPE_ASSUME_ROLE = "assume_role";


    public static final String MSG_TYPE_CREATE_POLICY = "create_policy";
    public static final String MSG_TYPE_DELETE_POLICY = "delete_policy";
    public static final String MSG_TYPE_UPDATE_POLICY = "update_policy";
    public static final String MSG_TYPE_LIST_POLICIES = "list_policies";
    public static final String MSG_TYPE_GET_POLICY = "get_policy";
    public static final String MSG_TYPE_LIST_ENTITIES_FOR_POLICY = "list_entities_for_policy";
    public static final String MSG_TYPE_ATTACH_ENTITY_POLICY = "attach_entity_policy";
    public static final String MSG_TYPE_DETACH_ENTITY_POLICY = "detach_entity_policy";
    public static final String MSG_TYPE_LIST_ENTITY_POLICIES = "list_entity_policies";
    public static final String MSG_TYPE_GET_ACCOUNT_AUTHORIZATION_DETAILS = "get_account_authorization_details";
    public static final String MSG_TYPE_POST_BACKUP_INFO = "post_backup_info";
    public static final String MSG_TYPE_UPDATE_DEFAULT_STRATEGY = "updateDefaultStrategy";
    public static final String ADMIN_ACCESS_KEY = "admin_access_key";
    public static final String ADMIN_ACCESS_AK = "MAKI0000000000000000";
    public static final String ADMIN_ACCESS_SK = "0000000000000000000000000000000000000000";

    public static final String MSG_TYPE_CHECK_LICENSE = "check_license";

    /*************redis数据返回成功状态******************/
    public static final String REDIS_OK = "OK";
    public static final long REDIS_0 = 0;

    /*************bucket 权限******************/
    public static final String PERMISSION_PRIVATE = "private";
    public static final String PERMISSION_SHARE_READ = "public-read";
    public static final String PERMISSION_SHARE_READ_WRITE = "public-read-write";

    public static final String PERMISSION_READ = "READ";
    public static final String PERMISSION_WRITE = "WRITE";
    public static final String PERMISSION_READ_CAP = "READ_ACP";
    public static final String PERMISSION_WRITE_CAP = "WRITE_ACP";
    public static final String PERMISSION_FULL_CON = "FULL_CONTROL";

    public static final String PERMISSION_READ_LONG = "x-amz-grant-read";
    public static final String PERMISSION_WRITE_LONG = "x-amz-grant-write";
    public static final String PERMISSION_READ_CAP_LONG = "x-amz-grant-read-acp";
    public static final String PERMISSION_WRITE_CAP_LONG = "x-amz-grant-write-acp";
    public static final String PERMISSION_FULL_CON_LONG = "x-amz-grant-full-control";

    public static final int PERMISSION_READ_NUM = 1;
    public static final int PERMISSION_WRITE_NUM = 2;
    public static final int PERMISSION_READ_CAP_NUM = 4;
    public static final int PERMISSION_WRITE_CAP_NUM = 8;
    public static final int PERMISSION_FULL_CON_NUM = 16;
    public static final int PERMISSION_SHARE_READ_NUM = 32;
    public static final int PERMISSION_SHARE_READ_WRITE_NUM = 64;
    public static final int PERMISSION_PRIVATE_NUM = 128;
    public static final int PERMISSION_CLEAR_GRANT_NUM = 0xe0;

    /*************object 权限******************/
    public static final String PERMISSION_BUCKET_OWNER_READ = "bucket-owner-read";
    public static final String PERMISSION_BUCKET_OWNER_FULL_CONTROL = "bucket-owner-full-control";

    public static final int OBJECT_PERMISSION_READ_NUM = 1;
    public static final String OBJECT_PERMISSION_READ = "1";
    public static final int OBJECT_PERMISSION_READ_CAP_NUM = 2;
    public static final String OBJECT_PERMISSION_READ_CAP = "2";
    public static final int OBJECT_PERMISSION_WRITE_CAP_NUM = 4;
    public static final String OBJECT_PERMISSION_WRITE_CAP = "4";
    public static final int OBJECT_PERMISSION_FULL_CON_NUM = 8;
    public static final String OBJECT_PERMISSION_FULL_CON = "8";

    public static final int OBJECT_PERMISSION_SHARE_READ_NUM = 16;
    public static final int OBJECT_PERMISSION_SHARE_READ_WRITE_NUM = 32;
    public static final int OBJECT_PERMISSION_SHARE_BUCKET_OWNER_READ_NUM = 64;
    public static final int OBJECT_PERMISSION_SHARE_OWNER_FULL_CONTROL_NUM = 128;
    public static final int OBJECT_PERMISSION_PRIVATE_NUM = 256;
    public static final int OBJECT_PERMISSION_CLEAR_GRANT_NUM = 496;

    /*************bucket 对象列表数据库文件后缀******************/
    public static final String LEVELDB_FIX = "-db";
    public static final String MONGODB_COLLECTION_PREFIX = "Collection";
    public static final String MONGODB_COLLECTION_UPLOADS = "Uploads";

    /*************元数据规格******************/
    public static final int META_SYS_MAX_SIZE = 1024;
    public static final int META_USR_MAX_SIZE = 2 * 1024;
    public static final int POLICY_MAX_SIZE = 5 << 12;

    /*-------------------------------账户相关-----------------------------------*/
    /*************Hash类型数据存储是判断类型的Key******************/
    public static final String USER_DATABASE_HASH_TYPE = "type";

    public static final String USER_DATABASE_ID_DEFAULT_AK = "null";
    public static final String USER_DATABASE_ID_AK1 = "access_key1";
    public static final String USER_DATABASE_ID_AK2 = "access_key2";
    public static final String USER_DATABASE_ID_NAME = "name";
    public static final String USER_DATABASE_ID_CREATE_TIME = "ctime";
    public static final String USER_DATABASE_ID_BUCKET_NUMBER = "bucket_nr";
    public static final String USER_DATABASE_ID_TYPE = "id";
    public static final String USER_TYPE = "account_type";
    public static final String ACCOUNT_DOMAIN = "domain";
    public static final String AD_ACCOUNT_TYPE = "Domain_AD";
    public static final String OPEN_LDAP_ACCOUNT_TYPE = "Domain_OPEN_LDAP";
    public static final String USER_DATABASE_AK_CREATE_TIME = "ctime";
    public static final String USER_DATABASE_AK_SK = "secret_key";
    public static final String USER_DATABASE_AK_ID = "id";
    public static final String USER_DATABASE_AK_TYPE = "ak";

    public static final String USER_DATABASE_NAME_PASSWD = "passwd";
    public static final String USER_DATABASE_NAME_ID = "id";
    public static final String USER_DATABASE_NAME_TYPE = "name";

    //代表一个用户拥有的bucket列表的set名字由<用户id>+<后缀>组成
    public static final String USER_BUCKET_SET_SUFFIX = "bucket_set";

    //账户配额
    public static final String USER_DATABASE_ACCOUNT_QUOTA_FLAG = "account_quota_flag";
    public static final String USER_DATABASE_ACCOUNT_QUOTA = "account_quota";
    public static final String USER_DATABASE_ACCOUNT_OBJNUM_FLAG = "account_objnum_flag";

    public static final String BUCKET_VERSION_QUOTA = "bucket-version-quota";

    public static final int BUCKET_VERSION_QUOTA_DEFAULT = 256;

    /*------------------------------Service相关---------------------------*/
    /**
     * 表示bucket可以被删除
     */
    public static final String DELETE_BUCKET_FLAG = "0";

    public static final String BUCKET_USER_NAME = "user_name";
    public static final String BUCKET_ACL = "acl";
    public static final String BUCKET_USER_ID = "user_id";
    public static final String BUCKET_VERSION_STATUS = "versionstatus";
    public static final String VERSION_SUSPENDED = "Suspended";
    public static final String VERSION_NUM = "versionnums";
    public static final String VERSION_LIMIT_SWITCH = "version_limit_switch";
    public static final String BACK_SOURCE = "back_source";
    public static final String BACK_SOURCE_REQUEST_HEADER = "back-source-request";

    /**
     * 生命周期相关
     */
    public static final String LIFECYCLE_RECORD = "_lifecycle_record";
    public static final String BACKUP_RECORD = "_backup_record";
    public static final String SERIALIZE_RECORD = "serialize_record";

    /**
     * redis中的节点信息
     */
    public static final String VNODE_TAKE_OVER = "take_over";
    public static final String VNODE_S_UUID = "s_uuid";
    public static final String VNODE_LUN_NAME = "lun_name";
    public static final String VNODE_ID = "vnode_id";

    /**
     * redis9中的分段信息
     */
    public static final String UPLOAD_NR = "upload_nr";
    public static final String UPLOAD_OBJECT_NAME = "key";
    public static final String OBJECT_TYPE = "obj_type";
    public static final String INITIATED = "initiated";

    /**
     * 显示的分段信息
     */
    public static final String PART_NUMBER_MAKER = "partNumberMarker";
    public static final String NEXT_PART_NUMBER_MAKER = "nextPartNumberMarker";
    public static final String PART_MAX_PARTS = "maxParts";
    public static final String TRUNCATED = "truncated";
    public static final String PART_LIST = "partList";
    public static final String PART_NUMBER = "partNumber";
    public static final String PART_SIZE = "partSize";
    public static final String PART_CONFIG = "part_config";
    public static final String ESCAPE_FLAG = "escape_flag";
    public static final String PART_CONFIG_SIGN = "part_config_sign";
    public static long UPLOAD_PART_MIN_SIZE = 5 * 1024 * 1024L;
    public static boolean ETAG_ESCAPE_FLAG = false;

    /**
     * 显示的用户元数据
     */
    public static final String META_ETAG = "ETag";
    public static final String META_SIZE = "size";
    public static final String META_LAST_MODIFIED = "last_modified";
    public static final String META_CONTENT_TYPE = "content_type";
    public static final String META_DATA = "metadata";

    /**
     * 桶配额相关
     */
    public static final String QUOTA_FLAG = "quota_flag";
    public static final String QUOTA_VALUE = "quota_value";
    public static final String OBJNUM_FLAG = "objnum_flag";

    /**
     * 跨域资源共享相关
     */
    public static final String CORS_CONFIG_KEY = "CORSConfiguration";
    public static final String BUCKET_TAG_KEY = "BucketTags";
    /************* ES相关 ******************/
    public static final String ES_INDEX = "moss";
    public static final String ES_SWITCH = "mda";
    public static final String ES_OBJ_NAME = "objName";
    public static final String ES_BUCKET = "bucketName";
    public static final String ES_ACCURATE = "accurate";
    public static final String ES_VAGUE = "vague";
    public static final String ES_PREFIX = "prefix";
    public static final String ES_SUFFIX = "suffix";
    public static final String ES_DATE = "date";
    public static final String ES_ON = "on";
    public static final String ES_META_TYPE = "metaType";
    public static final String ES_CONTENT_TYPE = "contentType";
    public static final String ES_ACCOUNT_ID = "accountId";
    public static final String ES_ETAG = "eTag";
    public static final String ES_SIZE = "objSize";
    public static final String ES_INODE = "inode";
    public static final String ES_DELETE_SOURCE = "deleteSource";
    public static final String ES_LINK_TYPE = "linkType";
    public static final String ES_TYPE_NORMAL = "normal";
    public static final String ES_TYPE_OR = "or";
    public static final String ES_TYPE_AND = "and";
    public static final String ES_SEARCH_LIST = "searchList";
    public static final String ES_BUCKET_SET = "es_bucket_set";
    public static final String ES_DELETE_ES = "deleteEs";

    public static final String METADATA_ANALYSIS = "metadata-analysis";
    public static final String OBJECT_LOCK_ENABLED = "object-lock-enabled-for-bucket";

    /************* 系统容量相关 *** redis REDIS_SYSINFO_INDEX中 ***************/
    public static final String SYS_CAP = "sys_cap";
    public static final String SYS_USED = "sys_used";

    /************ 性能配额相关 redis 表3****************/
    public static final String THROUGHPUT_QUOTA = "throughput_quota";
    public static final String BAND_WIDTH_QUOTA = "bandwidth_quota";
    public static final long DEFAULT_CHUNK_LIMIT = 1024 * 1024;
    public static final String DATA_SYNC_QUOTA = "sys_datasync_quota";

    public static final String STS_QUOTA = "sts_quota";

    public static final String DATASYNC_THROUGHPUT_QUOTA = "datasync_throughput_quota";
    public static final String DATASYNC_BAND_WIDTH_QUOTA = "datasync_bandwidth_quota";

    /************ 每个文件系统下存储对象文件的目录个数 ****************/
    public static final int FILESYSTEM_DIR_NUM = 10000;

    public static final int MAX_SMALL_SIZE = -1;

    public static final String ZERO_ETAG = "d41d8cd98f00b204e9800998ecf8427e";

    public static final String NULL = "null";

    /************ 多区域相关****************/
    public static final String MSG_TYPE_REGIONS_GET_REDIS_LOCK = "get_regions_redis_lock";
    public static final String MSG_TYPE_REGIONS_CREATE_BUCKET = "regions_create_bucket";
    public static final String MSG_TYPE_REGIONS_DELETE_BUCKET = "regions_delete_bucket";
    public static final String MSG_TYPE_REGIONS_BUCKET_TOETCD = "regions_bucket_to_etcd";
    public static final String MSG_TYPE_REGIONS_STATISTICS_SYNC = "regions_statistics_sync";
    public static final String MSG_TYPE_REGIONS_STATISTICS_MONTH = "regions_statistics_month";
    public static final String MULTI_REGION_TYPE_LOCK = "lock";
    public static final String MULTI_REGION_REQ_TYPE = "type";
    public static final String MULTI_REGION_DB = "db";
    public static final String MULTI_REGION_KEY = "key";
    public static final String MULTI_REGION_USER_ID = "user_id";
    public static final String DNS_NAME = "dns_name";
    public static final String DNS_NAME1 = "dns_name1";
    public static final String DNS_NAME2 = "dns_name2";
    public static final String BUSINESS_VLAN_NETMASK_SUFFIX= "_vlan_netmask";
    public static final String BUSINESS_VLAN_IPV6_NETMASK_SUFFIX= "_vlan_ipv6_netmask";
    public static final String BUSINESS_VLAN_DNS_SUFFIX= "_vlan_dns_name";
    public static final String DNS_NAME_PREFIX = "dns_name_prefix";
    public static final String OTHER_REGIONS = "other_regions";
    public static final String MULTI_REGION_LOCAL_REGION = "local_region";
    public static final String MULTI_REGION_DNS_INFO = "dns_info";
    public static final String MULTI_REGION_UPDATE_DNS_IP = "update_other_dns_ip";
    public static final String MULTI_SET_BUCKET_SYNC = "set_bucket_sync";
    public static final String MULTI_UPDATE_SERVER_AK_SK = "update_server_ak_sk";
    public static final String MULTI_CHANGE_SYNC_SSL = "change_sync_ssl";
    public static final String MULTI_CHECK_SYNC_SSL = "check_sync_ssl";
    public static final String MULTI_REGION_MODIFY_REMOTE_IPS = "modify_remote_ips";
    public static final String MULTI_REGION_INFO = "region_info";
    public static final String MULTI_REGION_ADD_MYSQL = "add_mysql";
    public static final String MULTI_REGION_CHECK_ENVIRONMENT = "checkRemoteEnv";
    public static final String MULTI_REGION_CHECK_LICENSE = "checkRegionLicense";
    public static final String MULTI_REGION_ZERO_TO_ONE = "zero_to_one";
    public static final String MULTI_REGION_ONE_TO_ZERO = "one_to_zero";
    public static final String MULTI_REGION_DEFAULT_REGION = "default_region";
    public static final String REGION_NAME = "region_name";
    public static final String REGION_CN_NAME = "cname";
    public static final String CLUSTER_NAMES = "cluster_names";
    public static final String CLUSTER_ROLE = "cluster_role";
    public static final String AIP = "aip";
    public static final String REGION_DNS_IPS = "dns_ips";
    public static final String REGION_FLAG = "regionFlag";
    public static final String REGION_FLAG_UPPER = "RegionFlag";
    public static final String ETCD_RECORD_HOST = "host";
    public static final String ETCD_RECORD_TTL = "ttl";
    public static final long TTL_VALUE = 30;
    public static final String DNS_TTL = "dns_ttl";
    public static final int TIMEOUT = 1;
    public static final String CORE_DNS_DIR_PREFIX = "/coredns/";
    public static final String DNS_PREFIX = "moss.";
    public static final String MASTER_DNS_UUID = "masterDnsuuid";

    /************ 双活相关****************/
    public static final String MSG_TYPE = "msg_type";
    public static final String MSG_TYPE_SITE_GET_REDIS_LOCK = "get_active_redis_lock";
    public static final String MSG_TYPE_SITE_CREATE_BUCKET = "active_create_bucket";
    public static final String MSG_TYPE_SITE_DELETE_BUCKET = "active_delete_bucket";
    public static final String MSG_TYPE_SITE_SET_ACL = "active_set_bucket_acl";
    public static final String MSG_TYPE_SITE_PUT_VERSION = "active_set_bucket_version";
    public static final String MSG_TYPE_SITE_PUT_METADATA = "active_set_bucket_metadata";
    public static final String MSG_TYPE_SITE_PUT_WORM = "active_set_bucket_worm";
    public static final String MSG_TYPE_SITE_PUT_INVENTORY = "active_set_bucket_inventory";
    public static final String MSG_TYPE_SITE_DEL_INVENTORY = "active_del_bucket_inventory";
    public static final String MSG_TYPE_SITE_PUT_LIFECYCLE = "active_set_bucket_lifecycle";
    public static final String MSG_TYPE_SITE_PUT_BACK_UP = "active_set_bucket_backup";
    public static final String MSG_TYPE_SITE_DEL_LIFECYCLE = "active_del_bucket_lifecycle";
    public static final String MSG_TYPE_SITE_DEL_BACKUP = "active_del_bucket_backup";
    public static final String MSG_TYPE_SITE_DEL_BUCKET_POLICY = "active_del_bucket_policy";
    public static final String MSG_TYPE_SITE_PUT_BUCKET_POLICY = "active_put_bucket_policy";
    public static final String MSG_TYPE_SITE_PUT_BUCKET_LOGGING = "active_put_bucket_logging";
    public static final String MSG_TYPE_SITE_PUT_BUCKET_NOTIFICATION = "active_put_bucket_notification";
    public static final String MSG_TYPE_SITE_PUT_BUCKET_TRASH = "active_put_bucket_trash";
    public static final String MSG_TYPE_SITE_DEL_BUCKET_TRASH = "active_del_bucket_trash";
    public static final String MSG_TYPE_SITE_PUT_BUCKET_QUOTA = "active_set_bucket_quota";
    public static final String MSG_TYPE_SITE_PUT_BUCKET_OBJECTS = "active_set_bucket_objects";
    public static final String MSG_TYPE_SITE_PUT_BUCKET_PERF_QUOTA = "active_set_bucket_perf_quota";
    public static final String MSG_TYPE_SITE_PUT_BUCKET_ENCRYPTION = "active_set_bucket_encryption";
    public static final String MSG_TYPE_SITE_DEL_BUCKET_ENCRYPTION = "active_del_bucket_encryption";
    public static final String MSG_TYPE_SITE_PUT_BUCKET_REFERER = "active_put_bucket_referer";
    public static final String MSG_TYPE_SITE_PUT_BUCKET_CLEAR_CONFIG = "active_put_bucket_clear_config";
    public static final String MSG_TYPE_SITE_DEL_BUCKET_CLEAR_CONFIG = "active_del_bucket_clear_config";
    public static final String MSG_TYPE_SITE_PUT_BUCKET_CORS = "active_put_bucket_cors";
    public static final String MSG_TYPE_SITE_DEL_BUCKET_CORS = "active_del_bucket_cors";
    public static final String MSG_TYPE_SITE_PUT_BUCKET_TAG = "active_put_bucket_tags";
    public static final String MSG_TYPE_SITE_DEL_BUCKET_TAG = "active_del_bucket_tags";
    public static final String MSG_TYPE_SITE_ASSUME_ROLE = "active_assume_role";
    public static final String MSG_TYPE_SITE_SET_AWS_SIGN = "active_set_aws_sign";
    public static final String MSG_TYPE_SITE_DELETE_AWS_SIGN = "active_delete_aws_sign";
    public static final String MSG_TYPE_PUT_FS_DIR_QUOTA_INFO = "active_put_fs_dir_quota_info";
    public static final String MSG_TYPE_PUT_FS_USER_QUOTA_INFO = "active_put_fs_user_quota_info";
    public static final String MSG_TYPE_PUT_FS_GROUP_QUOTA_INFO = "active_put_fs_group_quota_info";
    public static final String MSG_TYPE_PUT_MULTi_FS_PERFORMANCE_QUOTA = "active_put_multi_fs_performance_quota";
    public static final String MSG_TYPE_PUT_ADDRESS_PERFORMANCE_QUOTA = "active_put_address_performance_quota";
    public static final String MSG_TYPE_DEL_FS_QUOTA_INFO = "active_del_fs_quota_info";
    public static final String MSG_TYPE_SET_BUCKET_NFS = "active_set_bucket_nfs";
    public static final String MSG_TYPE_SET_BUCKET_CIFS = "active_set_bucket_cifs";
    public static final String MSG_TYPE_SET_BUCKET_FTP = "active_set_bucket_ftp";
    public static final String MSG_TYPE_ADD_NFS_IP_WHITELISTS = "active_add_nfs_ip_whitelists";
    public static final String MSG_TYPE_DEL_NFS_IP_WHITELISTS = "active_del_nfs_ip_whitelists";
    public static final String MSG_TYPE_SYNC_STS = "sync_sts";
    public static final String MSG_TYPE_SITE_SYNC = "active_sync";
    public static final String MASTER_CLUSTER = "master_cluster";
    public static final String LOCAL_CLUSTER = "local_cluster";
    public static final String TEMP_CLUSTERS = "temp_clusters";
    public static final String CLUSTER_NAME = "cluster_name";
    public static final String SYNC_ETH = "sync_eth";
    public static final String ETH_TYPE = "eth_type";
    public static final String CLUSTER_IPS = "iplist";
    public static final String SITE_NAME = "site";
    public static final String OTHER_CLUSTERS = "other_clusters";
    public static final String CHANGE_SYNC_SSL = "change_sync_ssl";
    /**
     * 正常状态为1。双活有一个站点异常，redis中的link_state改为0。
     */
    public static final String LINK_STATE = "link_state";
    public static final String EXTRA_LINK_STATE = "extra_link_state";
    public static final String SYNC_STATE = "sync_state";
    public static final String SITE_FLAG = "siteFlag";
    public static final String DOUBLE_FLAG = "doubleflag";
    public static final String SITE_FLAG_UPPER = "SiteFlag";
    public static final String SYS_DATA_SYNC_TIME = "sys_datasync_time";
    public static final String SYNC_START_TIME = "sync_start_time_";
    public static final String SYNC_END_TIME = "sync_end_time_";
    public static final String NOTIFY_SALVE_CLUSTER = "notifySalveCluster";
    public static final String ACTION_CREATE_BUCKET = "createBucket";
    public static final String ACTION_DELETE_BUCKET = "deleteBucket";
    public static final String ACTION_PUT_BUCKET_ACL = "putBucketAcl";
    public static final String ACTION_PUT_BUCKET_VERSION = "putVersion";
    public static final String ACTION_PUT_BUCKET_METADATA = "putMetadata";
    public static final String ACTION_PUT_BUCKET_WORM = "putBucketWorm";
    public static final String ACTION_PUT_BUCKET_INVENTORY = "putBucketInventory";
    public static final String ACTION_DEL_BUCKET_INVENTORY = "delBucketInventory";
    public static final String ACTION_PUT_BUCKET_LIFECYCLE = "putLifecycle";
    public static final String ACTION_PUT_BUCKET_BACKUP = "putBackupRule";
    public static final String ACTION_DEL_BUCKET_LIFECYCLE = "delLifecycle";
    public static final String ACTION_DEL_BUCKET_BACKUP = "delBackup";
    public static final String ACTION_PUT_BUCKET_POLICY = "putBucketPolicy";
    public static final String ACTION_DEL_BUCKET_POLICY = "delBucketPolicy";
    public static final String ACTION_PUT_BUCKET_LOGGING = "putBucketLogging";
    public static final String ACTION_PUT_BUCKET_NOTIFICATION = "putBucketNotification";
    public static final String ACTION_PUT_BUCKET_TRASH = "putBucketTrash";
    public static final String ACTION_DEL_BUCKET_TRASH = "delBucketTrash";
    public static final String ACTION_PUT_BUCKET_QUOTA = "putBucketQuota";
    public static final String ACTION_PUT_BUCKET_OBJECTS = "putBucketObjects";
    public static final String ACTION_PUT_BUCKET_PERF_QUOTA = "putBucketPerfQuota";
    public static final String ACTION_PUT_BUCKET_ENCRYPTION = "putBucketEncryption";
    public static final String ACTION_DEL_BUCKET_ENCRYPTION = "delBucketEncryption";
    public static final String ACTION_PUT_BUCKET_REFERER = "putBucketReferer";
    public static final String ACTION_PUT_BUCKET_CLEAR_CONFIG = "putBucketClearConfig";
    public static final String ACTION_DEL_BUCKET_CLEAR_CONFIG = "delBucketClearConfig";
    public static final String ACTION_PUT_BUCKET_CORS = "putBucketCors";
    public static final String ACTION_DEL_BUCKET_CORS = "delBucketCors";
    public static final String ACTION_PUT_BUCKET_TAG = "putBucketTags";
    public static final String ACTION_DEL_BUCKET_TAG = "delBucketTags";
    public static final String ACTION_SET_AWS_SIGN = "setAWSSign";
    public static final String ACTION_DELETE_AWS_SIGN = "deleteAWSSign";
    public static final String ACTION_ASSUME_ROLE = "assumeRole";
    public static final String ACTION_PUT_FS_DIR_QUOTA_INFO = "putFsDirQuotaInfo";
    public static final String ACTION_PUT_FS_USER_QUOTA_INFO = "putFsUserQuotaInfo";
    public static final String ACTION_PUT_FS_GROUP_QUOTA_INFO = "putFsGroupQuotaInfo";
    public static final String ACTION_DEL_FS_QUOTA_INFO = "delFsQuotaInfo";
    public static final String ACTION_PUT_MULTI_FS_PERFORMANCE_QUOTA = "putMultiFsPerformanceQuota";
    public static final String ACTION_PUT_ADDRESS_PERFORMANCE_QUOTA = "putAddressPerformanceQuota";
    public static final String ACTION_ADD_NFS_IP_WHITELISTS = "addNfsIpWhitelists";
    public static final String ACTION_DEL_NFS_IP_WHITELISTS = "delNfsIpWhitelists";
    public static final String ACTION_SET_BUCKET_NFS = "setBucketNfs";
    public static final String ACTION_SET_BUCKET_CIFS = "setBucketCifs";
    public static final String ACTION_SET_BUCKET_FTP = "setBucketFtp";
    public static final String ACL_HEADER = "syncObjAcl";
    public static final String USER_METADATA_HEADER = "syncObjUserMetaData";
    public static final String COPY_SOURCE_KEY = "SyncSourceKey";
    public static final String INNER_AUTH = "AWS aaaa1111bbbb2222cccc:000";
    public static final String SYNC_BUCKET_SET = "bucket_sync_set";
    public static final String NEXT_SYNC_TIME = "nextSyncTime";
    public static final String REMOVED_SYNC_BUCKET_SET = "removed_sync_bucket_set";
    public static final String CLUSTERS_STATUS = "clusters_status";
    public static final String OTHER_CLUSTERS_STATUS = "other_clusters_status";
    public static final String CLUSTERS_SYNC_STATE = "clusters_sync_state";
    public static final String NEED_SYNC_BUCKETS = "need_sync_buckets";
    public static final String CLOSED_BUCKETS = "closed_bucket";
    public static final String CLOSED_BUCKETS_MAP = "closed_bucket_map";
    public static final String NO_NEED_SYNC_BUCKETS = "no_need_sync_buckets";
    public static final String ETH2_STATUS = "eth2_status";
    public static final String ETH3_STATUS = "eth3_status";
    public static final String AVAILABLE_NODE_LIST = "available_node_list";
    /**
     * 0表示双活  1表示异步复制
     **/
    public static final String SYNC_POLICY = "sync_policy";
    public static final String SYNC_POLICY_ASYNC = "sync_policy_async";

    public static final String EXTRA_ASYNC_CLUSTER = "extra_async_cluster";
    public static final String EXTRA_ASYNC = "extra_async";
    public static final String EXTRA_CLUSTER_INFO = "extra_cluster_info";
    public static final String EXTRA_ASYNC_TYPE = "extra_type";
    // 磁带库
    public static final String TAPE_LIBRARY = "tape_library";

    public static final String CLUSTERS_SYNC_WAY = "cluster_sync_way";

    public static final String SYNC_STORAGE = "sync-storage";

    public static final String SYNC_PART_DATA = "sync-part-data";

    public static final String SYNC_VERSION_ID = "sync-version-id";

    // 站点B是复制站点时又部署了其他站点，需要处理在这个时间戳之前的差异记录
    public static final String ASYNC_TIME = "async_time";
    public static final String ASYNC_SYNC_COMPLETE_STR = "async_sync_complete";

    public static final String SYNC_COMPRESS = "sync_compress";

    public static final String GET_CHUNKS_INDEX = "get_chunks_index";

    /**
     * 假定客户端速度为1MB/s，此即为5G的对象上传完毕花费的秒数。
     */
    public static final int UPLOAD_MAX_INTERVAL = 5 * 1024;

    /**
     * 其他请求上传完毕允许的最大秒数。
     */
    public static final int MAX_INTERVAL = 100;

    public static final String ACTIVE_GET_SITE_INFO = "getSiteInfo";
    public static final String ACTIVE_GET_ADD_SITE_INFO = "getAddSiteInfo";
    public static final String ACTIVE_SET_SITE_INFO = "setSiteInfo";
    public static final String ACTIVE_SET_BACK_SITE_INFO = "setBackSiteInfo";
    public static final String ACTIVE_SET_SLAVE_SITE_INFO = "setSlaveSiteInfo";
    public static final String ACTIVE_CHECK_SYNC_BUCKET = "checkSyncBucket";
    public static final String ACTIVE_REMOTE_TEMP_SITE = "remoteTempSite";
    public static final String ACTIVE_ADD_MYSQL = "activeAddMysql";
    public static final String ACTIVE_ADD_EXTRA_MYSQL = "activeAddExtraMysql";
    public static final String ACTIVE_SYNC_IP = "syncIp";
    public static final String ACTIVE_SYNC_IP_STATE = "syncIpState";
    public static final String ACTIVE_GET_SITE_STORAGE_STRATEGY = "getSiteStorageStrategy";
    public static final String ACTIVE_UPDATE_SITE_DEFAULT_STRATEGY = "updateDefaultStrategy";
    public static final String ACTIVE_SET_EXTRA_SITE = "set_extra_site";

    /**
     * 桶开启数据同步开关
     */
    public static final String DATA_SYNC_SWITCH = "data-synchronization-switch";
    public static final String DELETE_SOURCE_SWITCH = "data-deleteSource-switch";
    public static final String SYNC_RELATION = "site-sync-relation";
    public static final String DELETE_SOURCE_SIGN = "data-deleteSource-sign";
    public static final String DELETE_SOURCE_TIME = "data-deleteSource-time";
    public static final String DEFAULT_DATA_SYNC_SWITCH = "default_data_sync_switch";
    public static final String LIFE_EXPIRATION_FLAG = "life-expiration-sign";
    public static final String ARCHIVE_INDEX = "archive-index";
    public static final String SYNC_INDEX = "sync-index";
    public static final String NFS_SWITCH = "nfs-switch";
    public static final String CIFS_SWITCH = "cifs-switch";
    public static final String FTP_SWITCH = "ftp-switch";
    public static final String SYNC_BUCKET_INDEX = "{\"0\":[1],\"1\":[0]}";
    public static final String ARCHIVE_BUCKET_INDEX = "{\"0\":[1],\"1\":[0]}";
    public static final Pattern HEX_ESCAPE = Pattern.compile("\\\\x[0-9a-fA-F]{2}");

    /**
     * 异构环境输入ak,sk
     */
    public static final String SERVER_AK = "server-ak";
    public static final String SERVER_SK = "server-sk";

    /**
     * 开启桶复制记录时间戳
     */
    public static final String BUCKET_SYNC_TIMESTAMP = "bucket_synchronization_timestamp";

    /**
     * 文件相关
     */
    public static final String NFS_ACL = "nfs-acl";
    public static final String CIFS_ACL = "cifs-acl";
    public static final String FTP_ACL = "ftp-acl";
    public static final String FS_STATUS = "status";
    public static final String FS_SQUASH = "squash";
    public static final String ANON_UID = "anonuid";
    public static final String ANON_GID = "anongid";
    public static final String GUEST = "guest";
    public static final String CASE_SENSITIVE = "case-sensitive";
    public static final String ANONYMOUS = "anonymous";
    public static final String NFS_IP_WHITE_LISTS = "nfs-ip-white-lists";

    /**
     * 桶清单
     */
    public static final String INVENTORY_SUFFIX = "_inventory";
    public static final String INVENTORY_SUFFIX_ID = "_inventory_id";
    public static final int INVENTORY_SCORE = 1;

    /**
     * 页面添加新站点的同步现状
     */
    public static final String DEPLOEY_RECORD = "deploy_record";

    /**
     * 开启桶复制的同步现状
     */
    public static final String BUCKET_DEPLOY_RECORD = "bucket_deploy_record";

    /**
     * 处理差异记录时的标志，表示需要额外写入async站点的差异记录。
     */
    public static final String ASYNC_CLUSTER_SIGNAL = "async_signal";
    public static final String HIS_SYNC_SIGNAL = "his_sync_signal";
    public static final String DELETE_SOURCE = "delete_source";
    public static final String UPDATE_ASYNC_RECORD = "update_async_RECORD";
    public static final String DELETE_ASYNC_RECORD = "delete_async_record";
    // 3复制使用
    public static final String DELETE_SYNC_RECORD = "delete_sync_record";
    public static final String SYNC_QOS_RULE = "sync_qos_rule";
    public static final String START_TIME = "startTime";
    public static final String END_TIME = "endTime";

    public static final Set<String> ASYNC_CLUSTER_SIGNAL_SET = new HashSet<>();

    static {
        ASYNC_CLUSTER_SIGNAL_SET.add(UPDATE_ASYNC_RECORD);
        ASYNC_CLUSTER_SIGNAL_SET.add(DELETE_ASYNC_RECORD);
        ASYNC_CLUSTER_SIGNAL_SET.add(DELETE_SYNC_RECORD);
        ASYNC_CLUSTER_SIGNAL_SET.add(ArchiveAnalyzer.ARCHIVE_ANALYZER_KEY);
    }

    /**
     * 桶策略标签
     */
    public static final String POLICY_VERSION = "Version";
    public static final String POLICY_Statement = "Statement";
    public static final String POLICY_SID = "Sid";
    public static final String POLICY_PRINCIPAL = "Principal";
    public static final String POLICY_NOT_PRINCIPAL = "NotPrincipal";
    public static final String POLICY_ACTION = "Action";
    public static final String POLICY_NOT_ACTION = "NotAction";
    public static final String POLICY_RESOURCE = "Resource";
    public static final String POLICY_NOT_RESOURCE = "NotResource";
    public static final String POLICY_CONDITION = "Condition";
    public static final String RESOURCE_PREFIX = "mrn::moss:::";
//    public static final String S3_RESOURCE_PREFIX = "arn:aws:s3:::";
    public static final String PRINCIPAL_PREFIX = "mrn::iam::";
    public static final String ACTION_PREFIX = "moss:";
    public static final String POLICY_EFFECT = "Effect";
    public static final String POLICY_SUFFIX = "_policys";
    public static final String POLICY_X_AMZ_PREFIX = "id=";
    public static final String POLICY_STRING_PERMISSION = "policyPermissions";

    /**
     * Condition标签
     */
    // IP类型
    public static final String POLICY_IP_ADDRESS = "IpAddress";
    public static final String POLICY_NOT_IP_ADDRESS = "NotIpAddress";
    // 字符串类型
    public static final String POLICY_STRING_EQUALS = "StringEquals";
    public static final String POLICY_STRING_NOT_EQUALS = "StringNotEquals";
    public static final String POLICY_STRING_LIKE = "StringLike";
    public static final String POLICY_STRING_NOT_LIKE = "StringNotLike";
    public static final String POLICY_STRING_EQUALS_IGNORE_CASE = "StringEqualsIgnoreCase";
    public static final String POLICY_STRING_NOT_EQUALS_IGNORE_CASE = "StringNotEqualsIgnoreCase";
    // 数值类型
    public static final String POLICY_NUMERIC_EQUALS = "NumericEquals";
    public static final String POLICY_NUMERIC_NOT_EQUALS = "NumericNotEquals";
    public static final String POLICY_NUMERIC_LESS_THAN = "NumericLessThan";
    public static final String POLICY_NUMERIC_LESS_THAN_EQUALS = "NumericLessThanEquals";
    public static final String POLICY_NUMERIC_GREATER_THAN = "NumericGreaterThan";
    public static final String POLICY_NUMERIC_GREATER_THAN_EQUALS = "NumericGreaterThanEquals";
    // 日期类型
    public static final String POLICY_DATE_EQUALS = "DateEquals";
    public static final String POLICY_DATE_NOT_EQUALS = "DateNotEquals";
    public static final String POLICY_DATE_LESS_THAN = "DateLessThan";
    public static final String POLICY_DATE_LESS_THAN_EQUALS = "DateLessThanEquals";
    public static final String POLICY_DATE_GREATER_THAN = "DateGreaterThan";
    public static final String POLICY_DATE_GREATER_THAN_EQUALS = "DateGreaterThanEquals";
    // 布尔类型
    public static final String POLICY_BOOL = "Bool";
    // 该标签不区分大小写
    public static final String POLICY_SOURCE_IP = "SourceIp";
    public static final String POLICY_STRING_PREFIX = "prefix";
    public static final String POLICY_STRING_DELIMITER = "delimiter";
    public static final String POLICY_STRING_LOCATION_CONSTRAINT = "locationconstraint";
    public static final String POLICY_STRING_SIGNATURE_VERSION = "signatureversion";
    public static final String POLICY_STRING_VERSION_ID = "versionid";
    public static final String POLICY_STRING_USER_AGENT = "UserAgent";
    public static final String POLICY_STRING_X_AMZ_ACL = "x-amz-acl";
    public static final String POLICY_STRING_X_AMZ_SHA256 = "x-amz-content-sha256";
    public static final String POLICY_STRING_X_AMZ_FULL = "x-amz-grant-full-control";
    public static final String POLICY_STRING_X_AMZ_READ = "x-amz-grant-read";
    public static final String POLICY_STRING_X_AMZ_WRITE = "x-amz-grant-write";
    public static final String POLICY_STRING_X_AMZ_READ_ACP = "x-amz-grant-read-acp";
    public static final String POLICY_STRING_X_AMZ_WRITE_ACP = "x-amz-grant-write-acp";
    public static final String POLICY_STRING_X_AMZ_COPY_SOURCE = "x-amz-copy-source";
    public static final String POLICY_STRING_X_AMZ_DIRECTIVE = "x-amz-metadata-directive";
    public static final String POLICY_STRING_X_AMZ_ENCRYPTION = "x-amz-server-side-encryption";
    public static final String POLICY_STRING_LOCK_MODE = "object-lock-mode";

    public static final String POLICY_DATE_CURRENT_TIME = "CurrentTime";
    public static final String POLICY_DATE_WORM_DATE = "object-lock-retain-until-date";


    public static final String POLICY_NUMERIC_MAX_KEYS = "max-keys";

    public static final String POLICY_BOOL_SECURE_TRANSPORT = "SecureTransport";
    /**
     * statement中可能存在的元素集合
     */
    public static final List<String> STATEMENT_ELEMENT_LIST = Arrays.asList(POLICY_SID, POLICY_PRINCIPAL, POLICY_NOT_PRINCIPAL, POLICY_ACTION,
            POLICY_NOT_ACTION, POLICY_RESOURCE, POLICY_NOT_RESOURCE, POLICY_EFFECT, POLICY_CONDITION);
    /**
     * Condition中可能存在的元素集合
     */
    public static final List<String> CONDITION_ELEMENT_LIST = Arrays.asList(POLICY_IP_ADDRESS, POLICY_NOT_IP_ADDRESS, POLICY_STRING_LIKE, POLICY_STRING_NOT_LIKE, POLICY_STRING_EQUALS, POLICY_STRING_NOT_EQUALS,
            POLICY_STRING_EQUALS_IGNORE_CASE, POLICY_STRING_NOT_EQUALS_IGNORE_CASE, POLICY_NUMERIC_EQUALS, POLICY_NUMERIC_NOT_EQUALS, POLICY_NUMERIC_GREATER_THAN, POLICY_NUMERIC_GREATER_THAN_EQUALS,
            POLICY_NUMERIC_LESS_THAN, POLICY_NUMERIC_LESS_THAN_EQUALS, POLICY_DATE_EQUALS, POLICY_DATE_NOT_EQUALS, POLICY_DATE_LESS_THAN, POLICY_DATE_LESS_THAN_EQUALS, POLICY_DATE_GREATER_THAN,
            POLICY_DATE_GREATER_THAN_EQUALS, POLICY_BOOL);

    /**
     * Condition中IP类型可能存在的元素集合
     */
    public static final List<String> CONDITION_ELEMENT_IP_LIST = Collections.singletonList(POLICY_SOURCE_IP);

    /**
     * Condition中String类型可能存在的元素集合
     */
    public static final List<String> CONDITION_ELEMENT_STRING_LIST = Arrays.asList(POLICY_STRING_PREFIX, POLICY_STRING_DELIMITER, POLICY_STRING_LOCATION_CONSTRAINT, POLICY_STRING_SIGNATURE_VERSION, POLICY_STRING_LOCK_MODE,
            POLICY_STRING_VERSION_ID, POLICY_STRING_USER_AGENT, POLICY_STRING_X_AMZ_SHA256, POLICY_STRING_X_AMZ_ACL, POLICY_STRING_X_AMZ_COPY_SOURCE, POLICY_STRING_X_AMZ_DIRECTIVE, POLICY_STRING_X_AMZ_FULL, POLICY_STRING_X_AMZ_READ, POLICY_STRING_X_AMZ_READ_ACP,
            POLICY_STRING_X_AMZ_WRITE, POLICY_STRING_X_AMZ_WRITE_ACP, POLICY_STRING_X_AMZ_ENCRYPTION);

    /**
     * Condition中Date类型可能存在的元素集合
     */
    public static final List<String> CONDITION_ELEMENT_DATA_LIST = Arrays.asList(POLICY_DATE_CURRENT_TIME, POLICY_DATE_WORM_DATE);

    /**
     * Condition中NUMERIC类型可能存在的元素集合
     */
    public static final List<String> CONDITION_ELEMENT_NUMERIC_LIST = Collections.singletonList(POLICY_NUMERIC_MAX_KEYS);

    /**
     * Condition中Bool类型可能存在的元素集合
     */
    public static final List<String> CONDITION_ELEMENT_BOOL_LIST = Collections.singletonList(POLICY_BOOL_SECURE_TRANSPORT);

    /**
     * 因为桶策略太长,将其分组存放
     */
    public static final String[] STATEMENTS_POLICES = {"policy0", "policy1", "policy2",
            "policy3", "policy4", "policy5", "policy6", "policy7", "policy8"};

    public static final String COMPONENT_USER_ID = "component_user_id";

    //aws iam 路由
    public static final String IAM_ROUTE = "POST/";

    //swift 相关
    public static final String SECRET = "123456789abcd";

    public static final long TTL_MILLIS = 60 * 60 * 1000L;

    /*****Rabbitmq存储池相关队列中标识存储池名称结束的字符串*****/
    public static final String END_STR = "#";

    /*****域控相关字段*****/
    public static final String LDAP_CONFIG_KEY = "ldap_config";
    public static final String LDAP_DOMAIN_NAME = "domainName";

    public static final String DICOM_COMPRESS_SWITCH = "dicom-compress-switch";
    public static final String DICOM_COMPRESS_BUCKET_KEY_PREFIX = "dicom_compress_bucket_";
}
