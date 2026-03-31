package com.macrosan.filesystem;


public class FsConstants {
    public static String ADMIN_ACCOUNT = "admin";
    public static String ADMIN_PASSWD = "admin";
    public static String ADMIN_S3ID = "0";
    public static String GUEST_ID = "guestId";
    public static final String BUCKET_CASE_SENSITIVE = "caseSensitive";

    public final static int OK = 0;
    public final static int ENOENT = 2;       // 不存在
    public final static int EIO = 5;          // IO错误 一般的未知错误都返回EIO
    public final static int EEXIST = 17;      // 已经存在
    public final static int ENOSYS = 38;      // 未定义的方法
    public final static int ENOTEMPTY = 39;   // 目录下非空
    public final static int ENODATA = 61;     // 无可利用数据
    public final static int NFS_MAX_NAME_LENGTH = 1024; //最大文件名长度为1024
    public final static int SMB_MAX_FILE_NAME_LENGTH = 255; //SMB 不包含路径，纯文件名最大为255个字符
    public final static long ONE_SECOND_NANO = 1_000_000_000; //1s 相当于 1_000_000_000 ns，用于精确时间计算

    public static final int S_IFDIR = 0040000;
    public static final int S_IFREG = 0100000;
    public static final int S_IFLNK = 0120000;
    public static final int S_IFMT = 00170000;
    public static final int S_IFBLK = 060000; //块设备
    public static final int S_IFCHR = 020000; //字符设备
    public static final int S_IFFIFO = 010000;//命名管道（FIFO）
    public static final int S_IFSOCK = 0140000;//套接字文件
    public static final int DEFAULT_MODE = 0744;
    public static final int DEFAULT_DIR_MODE = 0775;//默认cifs创建的目录ugo权限，以umask=002
    public static final int DEFAULT_FILE_MODE = 0664;//默认cifs创建的文件ugo权限，以umask=002
    public static final int OBJECT_TRANS_FILE_MODE = 0777;

    public static class UnmountFlags {
        public static final int MNT_FORCE = 0x00000001;
        public static final int MNT_DETACH = 0x00000002;
        public static final int MNT_EXPIRE = 0x00000004;
        public static final int UMOUNT_NOFOLLOW = 0x00000008;
    }

    public static class MountFlags {
        public static final int MS_RDONLY = 1;
        public static final int MS_NOSUID = 2;
        public static final int MS_NODEV = 4;
        public static final int MS_NOEXEC = 8;
        public static final int MS_SYNCHRONOUS = 16;
        public static final int MS_REMOUNT = 32;
        public static final int MS_MANDLOCK = 64;
        public static final int MS_DIRSYNC = 128;
        public static final int MS_NOATIME = 1024;
        public static final int MS_NODIRATIME = 2048;
        public static final int MS_BIND = 4096;
        public static final int MS_MOVE = 8192;
        public static final int MS_REC = 16384;
        public static final int MS_SILENT = 32768;
        public static final int MS_POSIXACL = (1 << 16);
        public static final int MS_UNBINDABLE = (1 << 17);
        public static final int MS_PRIVATE = (1 << 18);
        public static final int MS_SLAVE = (1 << 19);
        public static final int MS_SHARED = (1 << 20);
        public static final int MS_RELATIME = (1 << 21);
        public static final int MS_KERNMOUNT = (1 << 22);
        public static final int MS_I_VERSION = (1 << 23);
        public static final int MS_STRICTATIME = (1 << 24);
        public static final int MS_NOREMOTELOCK = (1 << 27);
        public static final int MS_NOSEC = (1 << 28);
        public static final int MS_BORN = (1 << 29);
        public static final int MS_ACTIVE = (1 << 30);
        public static final int MS_NOUSER = (1 << 31);
    }

    public static class OpenFlags {
        public static final int O_RDONLY = 00000000;
        public static final int O_WRONLY = 00000001;
        public static final int O_RDWR = 00000002;
        public static final int O_CREAT = 00000100;
        public static final int O_EXCL = 00000200;
        public static final int O_NOCTTY = 00000400;
        public static final int O_TRUNC = 00001000;
        public static final int O_APPEND = 00002000;
        public static final int O_NONBLOCK = 00004000;
        public static final int O_DSYNC = 00010000;
        public static final int O_DIRECT = 00040000;
        public static final int O_LARGEFILE = 00100000;
        public static final int O_DIRECTORY = 00200000;
        public static final int O_NOFOLLOW = 00400000;
        public static final int O_NOATIME = 01000000;
        public static final int O_CLOEXEC = 02000000;
        public static final int O_SYNC = 04000000;
    }

    public static class InitFlags {
        public static final int CAP_ASYNC_READ = (1 << 0);
        public static final int CAP_POSIX_LOCKS = (1 << 1);
        public static final int CAP_FILE_OPS = (1 << 2);
        public static final int CAP_ATOMIC_O_TRUNC = (1 << 3);
        public static final int CAP_EXPORT_SUPPORT = (1 << 4);
        public static final int CAP_BIG_WRITES = (1 << 5);
        public static final int CAP_DONT_MASK = (1 << 6);
        public static final int CAP_SPLICE_WRITE = (1 << 7);
        public static final int CAP_SPLICE_MOVE = (1 << 8);
        public static final int CAP_SPLICE_READ = (1 << 9);
        public static final int CAP_FLOCK_LOCKS = (1 << 10);
        public static final int CAP_IOCTL_DIR = (1 << 11);
        public static final int CAP_AUTO_INVAL_DATA = (1 << 12);
        public static final int CAP_READDIRPLUS = (1 << 13);
        //不设置则表示默认readdir都是用readdirplus
        public static final int CAP_READDIRPLUS_AUTO = (1 << 14);
        public static final int CAP_ASYNC_DIO = (1 << 15);
        public static final int CAP_WRITEBACK_CACHE = (1 << 16);
        public static final int CAP_NO_OPEN_SUPPORT = (1 << 17);
        public static final int CAP_PARALLEL_DIROPS = (1 << 18);
        public static final int CAP_HANDLE_KILLPRIV = (1 << 19);
        public static final int CAP_POSIX_ACL = (1 << 20);
        public static final int CAP_ABORT_ERROR = (1 << 21);
        public static final int CAP_MAX_PAGES = (1 << 22);
        public static final int CAP_CACHE_SYMLINKS = (1 << 23);
        public static final int CAP_NO_OPENDIR_SUPPORT = (1 << 24);
        public static final int CAP_EXPLICIT_INVAL_DATA = (1 << 25);
    }

    public static class SetAttrFlags {
        public static final int FUSE_SET_ATTR_MODE = (1 << 0);
        public static final int FUSE_SET_ATTR_UID = (1 << 1);
        public static final int FUSE_SET_ATTR_GID = (1 << 2);
        public static final int FUSE_SET_ATTR_SIZE = (1 << 3);
        public static final int FUSE_SET_ATTR_ATIME = (1 << 4);
        public static final int FUSE_SET_ATTR_MTIME = (1 << 5);
        public static final int FUSE_SET_ATTR_ATIME_NOW = (1 << 7);
        public static final int FUSE_SET_ATTR_MTIME_NOW = (1 << 8);
        public static final int FUSE_SET_ATTR_CTIME = (1 << 10);
    }

    public static class Rename2Flags {
        public static final int RENAME_NOREPLACE = (1 << 0);
        public static final int RENAME_EXCHANGE = (1 << 1);
        //union/overlay filesystem 的删除标记
        public static final int RENAME_WHITEOUT = (1 << 2);
    }

    public static class LockFlags {
        //flock
        public static final int FUSE_LK_FLOCK = 1;
    }

    public static class LockOpt {
        //lock
        public static final int F_RDLCK = 0;
        public static final int F_WRLCK = 1;
        public static final int F_UNLCK = 2;
    }

    public static class FLockOpt {
        //数值和c的头文件不同
        public static final int LOCK_SH = 0;
        public static final int LOCK_EX = 1;
        public static final int LOCK_UN = 2;

        //        public static final int LOCK_NB = 4;
        public static final int LOCK_MAND = 32;
        public static final int LOCK_READ = 64;
        public static final int LOCK_WRITE = 128;
        public static final int LOCK_RW = 192;
    }

    public static class IoctlOpt {
        public static final int FUSE_IOCTL_COMPAT = (1 << 0);
        public static final int FUSE_IOCTL_UNRESTRICTED = (1 << 1);
        public static final int FUSE_IOCTL_RETRY = (1 << 2);
        public static final int FUSE_IOCTL_32BIT = (1 << 3);
        public static final int FUSE_IOCTL_DIR = (1 << 4);
        public static final int FUSE_IOCTL_COMPAT_X32 = (1 << 5);
        public static final int FUSE_IOCTL_MAX_IOV = 256;
    }

    public static class FileType {
        public static final int NF_NON = 0;
        public static final int NF_REG = 1;
        public static final int NF_DIR = 2;
        public static final int NF_BLK = 3;
        public static final int NF_CHR = 4;
        public static final int NF_LINK = 5;
        public static final int NF_SOCK = 6;
        public static final int NF_FIFO = 7;
    }

    public static class NfsErrorNo {
        public static final int NFS3_OK = 0;
        public static final int NFS3ERR_PERM = 1;
        public static final int NFS3ERR_NOENT = 2;
        public static final int NFS3ERR_I0 = 5;
        public static final int NFS3ERR_NXIO = 6;
        public static final int NFS3ERR_ACCES = 13;
        public static final int NFS3ERR_EXIST = 17;
        public static final int NFS3ERR_XDEV = 18;
        public static final int NFS3ERR_NODEV = 19;
        public static final int NFS3ERR_NOTDIR = 20;
        public static final int NFS3ERR_ISDIR = 21;
        public static final int NFS3ERR_INVAL = 22;
        public static final int NFS3ERR_FBIG = 27;
        public static final int NFS3ERR_NOSPC = 28;
        public static final int NFS3ERR_ROFS = 30;
        public static final int NFS3ERR_MLINK = 31;
        public static final int NFS3ERR_NAMETOOLONG = 63;
        public static final int NFS3ERR_NOTEMPTY = 66;
        public static final int NFS3ERR_DQUOT = 69;
        public static final int NFS3ERR_STALE = 70;
        public static final int NFS3ERR_REMOTE = 71;
        public static final int NFS3ERR_BADHANDLE = 10001;
        public static final int NFS3ERR_NOT_SYNC = 10002;
        public static final int NFS3ERR_BAD_COOKIE = 10003;
        public static final int NFS3ERR_NOTSUPP = 10004;
        public static final int NFS3ERR_TOOSMALL = 10005;
        public static final int NFS3ERR_SERVERFAULT = 10006;
        public static final int NFS3ERR_BADTYPE = 10007;
        public static final int NFS3ERR_JUKEBOX = 10008;



        public static final int NFS4ERR_DELAY = 10008;
        public static final int NFS4ERR_SAME = 10009;
        public static final int NFS4ERR_DENIED = 10010;
        public static final int NFS4ERR_EXPIRED = 10011;
        public static final int NFS4ERR_LOCKED = 10012;
        public static final int NFS4ERR_GRACE = 10013;
        public static final int NFS4ERR_SHARE_DENIED = 10015;
        public static final int NFS4ERR_CLID_INUSE = 10017;
        public static final int NFS4ERR_NOFILEHANDLE = 10020;
        public static final int NFS4ERR_STALE_CLIENTID = 10022;
        public static final int NFS4ERR_OLD_STATEID = 10024;
        public static final int NFS4ERR_BAD_STATEID = 10025;
        public static final int NFS4ERR_BAD_SEQID = 10026;
        public static final int NFS4ERR_NOT_SAME = 10027;
        public static final int NFS4ERR_LOCK_RANGE = 10028;
        public static final int NFS4ERR_SYMLINK = 10029;
        public static final int NFS4ERR_RESTOREFH = 10030;
        public static final int NFS4ERR_NO_GRACE = 10033;
        public static final int NFS4ERR_BADXDR = 10036;
        public static final int NFS4ERR_LOCKS_HELD = 10037;
        public static final int NFS4ERR_OPENMODE = 10038;
        public static final int NFS4ERR_BADNAME = 10041;
        public static final int NFS4ERR_BAD_RANGE = 10042;
        public static final int NFS4ERR_FILE_OPEN = 10046;
        public static final int NFS4ERR_BADSESSION = 10052;
        public static final int NFS4ERR_BADSLOT = 10053;
        public static final int NFS4ERR_COMPLETE_ALREADY = 10054;
        public static final int NFS4ERR_CONN_NOT_BOUND_TO_SESSION = 10055;
        public static final int NFS4ERR_SEQ_MISORDERED = 10063;
        public static final int NFS4ERR_CLIENTID_BUSY = 10074;
        public static final int NFS4ERR_DEADSESSION	= 10078;
        public static final int NFS4ERR_WRONG_TYPE = 10083;


    }

    public static class NFSACLOpCode {
        public static final int NFSACLPROC_NULL = 0;
        public static final int NFSACLPROC_GETACL = 1;
        public static final int NFSACLPROC_SETACL = 2;
    }

    public static class NFSQuotaOpCode {
        public static final int NFSQUOTAPROC_GETQUOTA = 1;
        public static final int NFSQUOTAPROC_SETQUOTA = 3;
    }

    public static class NFSQuotaStatus {
        public static final int Q_OK = 1;    /* quota returned */
        public static final int Q_NOQUOTA = 2;  /* noquota for uid */
        public static final int Q_EPERM = 3;    /* no permission to access quota */
    }

    /**
     * NFS非标准ACL协议中的acl类型
     **/
    public static class NFSACLType {
        public static final String NFS_ACE = "NFS_ACE";
        public static final String CIFS_CREATE = "CIFS_CREATE";
        public static final String TRANSFER_S3DIR = "TRANSFER_S3DIR";

        // 文件所有者对当前文件的权限
        public static final int NFSACL_USER_OBJ = 1;
        // 特定用户对当前文件的权限;
        public static final int NFSACL_USER = 2;
        // 文件所属组对当前文件的权限
        public static final int NFSACL_GROUP_OBJ = 4;
        // 特定组对当前文件的权限
        public static final int NFSACL_GROUP = 8;
        // 掩码权限，acl设置的权限不会超过掩码权限
        public static final int NFSACL_CLASS = 16;
        // 其它用户对当前文件的权限
        public static final int NFSACL_OTHER = 32;

        // 当前目录下子文件或目录继承权限，目录所有者对当前目录的权限
        public static final int DEFAULT_NFSACL_USER_OBJ = 4097;
        // 当前目录下子文件或目录继承权限，特定用户对当前目录的权限
        public static final int DEFAULT_NFSACL_USER = 4098;
        // 当前目录下子文件或目录继承权限，目录所属组对当前目录的权限
        public static final int DEFAULT_NFSACL_GROUP_OBJ = 4100;
        // 当前目录下子文件或目录继承权限，特定组对当前目录的权限
        public static final int DEFAULT_NFSACL_GROUP = 4104;
        // 当前目录下子文件或目录继承权限，不可超过的掩码权限
        public static final int DEFAULT_NFSACL_CLASS = 4112;
        // 当前目录下子文件或目录继承权限，其它用户对当前目录的权限
        public static final int DEFAULT_NFSACL_OTHER = 4128;

        //属组元素顺序不可更改
        public static final int[] UGO_ACE_ARR = {NFSACL_USER_OBJ, NFSACL_GROUP_OBJ, NFSACL_OTHER};
        public static final int[] DEFAULT_UGO_ACE_ARR = {DEFAULT_NFSACL_USER_OBJ, DEFAULT_NFSACL_GROUP_OBJ, DEFAULT_NFSACL_OTHER};
    }

    public static class NFSAccessAcl {
        // Read data from file or read a directory
        public static final int READ = 0x01; // 1

        // Look up a name in a directory (no meaning for non-directory objects)
        public static final int LOOK_UP = 0x02;  // 2

        // Rewrite existing file data or modify existing directory entries.
        public static final int MODIFY = 0x04;  // 4

        // Write new data or add directory entries
        public static final int EXTEND = 0x08;  // 8

        // Delete an existing directory entry
        public static final int DELETE = 0x10;  // 16

        // Execute file (no meaning for a directory)
        public static final int EXECUTE = 0x20;  // 32

        public static final int ALL_RIGHT = (READ | LOOK_UP | MODIFY | EXTEND | DELETE | EXECUTE);
    }

    public static class RpcAuthType{
        public static final int RPC_AUTH_NULL = 0;
        public static final int RPC_AUTH_UNIX = 1;
        public static final int RPC_AUTH_SHORT = 2;
        public static final int RPC_AUTH_DES = 3;
        public static final int RPC_AUTH_KRB = 4;
        public static final int RPC_AUTH_GSS = 6;
        public static final int RPC_AUTH_MAXFLAVOR = 8;
        public static final int RPC_AUTH_GSS_KRB5 = 390003;
        public static final int RPC_AUTH_GSS_KRB5I = 390004;
        public static final int RPC_AUTH_GSS_KRB5P = 390005;
        public static final int RPC_AUTH_GSS_LKEY = 390006;
        public static final int RPC_AUTH_GSS_LKEYI = 390007;
        public static final int RPC_AUTH_GSS_LKEYP = 390008;
        public static final int RPC_AUTH_GSS_SPKM = 390009;
        public static final int RPC_AUTH_GSS_SPKMI = 390010;
        public static final int RPC_AUTH_GSS_SPKMP = 390011;
    }

    public static class GSSType{
        public static final int RPC_GSS_SVC_NONE = 1;
        public static final int RPC_GSS_SVC_INTEGRITY = 2;
        public static final int RPC_GSS_SVC_PRIVACY = 3;
    }

    public static class NFS4Type{
        public static final int NF4BAD = 0;
        public static final int NF4REG = 1;
        public static final int NF4DIR = 2;
        public static final int NF4BLK = 3;
        public static final int NF4CHR = 4;
        public static final int NF4LNK = 5;
        public static final int NF4SOCK = 6;
        public static final int NF4FIFO = 7;
        public static final int NF4ATTRDIR = 8;
        public static final int NF4NAMEDATTR = 9;
    }


    public static final int NFS3_FHSIZE = 64;
    public static final int NFS3_COOKIEVERFSIZE = 8;
    public static final int NFS3_CREATEVERFSIZE = 8;
    public static final int NFS3_WRITEVERFSIZE = 8;
    public static final int NFS4_LEASE_TIME = 90;
    public final static int NFS4_MAX_SESSION_SLOTS = 16;
    public final static int NFS4_MAX_OPS = 128;
    public final static int NFS4_CALLBACK_PROGRAM = 0x40000000;

    public static final byte NBSSMessage = 0x0;
    public static final byte NBSSRequest = (byte) 0x81;
    public static final byte NBSSPositive = (byte) 0x82;
    public static final byte NBSSNegative = (byte) 0x83;
    public static final byte NBSSRetarget = (byte) 0x84;
    public static final byte NBSSKeepalive = (byte) 0x85;
    public static final byte NSSSSessionKeepalive = (byte) 0x89;

    // https://learn.microsoft.com/zh-cn/openspecs/windows_protocols/ms-erref/596a1078-e883-4972-9bbc-49e60bebca55
    public static class NTStatus {
        public static final int STATUS_SUCCESS = 0x00000000;
        public static final int STATUS_NOT_IMPLEMENTED = 0xc0000002;
        public static final int STATUS_INVALID_INFO_CLASS = 0xC0000003;
        public static final int STATUS_INVALID_DEVICE_REQUEST = 0xc0000016;
        public static final int STATUS_BAD_LOGON_SESSION_STATE = 0xC0000104;
        public static final int STATUS_LOGON_FAILURE = 0xC000006d;
        public static final int STATUS_NETWORK_SESSION_EXPIRED = 0xC000035C;
        public static final int STATUS_NETWORK_NAME_DELETED = 0xC00000C9;
        public static final int STATUS_ACCESS_DENIED = 0xC0000022;
        public static final int STATUS_INVALID_PARAMETER = 0xC000000D;
        public static final int STATUS_INSUFFICIENT_RESOURCES = 0xC000009A;
        public static final int STATUS_TIMEOUT = 0x00000102;
        public static final int STATUS_NOT_SUPPORTED = 0x00bb;
        public static final int STATUS_NO_MORE_FILES = 0x80000006;
        public static final int STATUS_NOT_SAME_DEVICE = 0xC00000D4;
        public static final int STATUS_INFO_LENGTH_MISMATCH = 0xC0000004;
        public static final int STATUS_NOT_FOUND = 0xC0000225;
        public static final int STATUS_OBJECT_PATH_NOT_FOUND = 0xC000003A;

        public static final int STATUS_OBJECT_NAME_NOT_FOUND = 0xc0000034;
        public static final int STATUS_DATA_ERROR = 0xc000003E;
        public static final int STATUS_DIRECTORY_NOT_EMPTY = 0xc0000101;
        public static final int STATUS_NO_SUCH_FILE = 0xC000000F;
        public static final int STATUS_NO_SUCH_USER = 0xC0000064;
        public static final int STATUS_OBJECT_NAME_EXISTS = 0x40000000;
        public static final int STATUS_OBJECT_NAME_COLLISION = 0xC0000035;
        public static final int STATUS_FILE_TOO_LARGE = 0xC0000904;
        public static final int STATUS_NAME_TOO_LONG = 0xC0000106;
        public static final int STATUS_OPEN_FAILED = 0xC0000136;
        public static final int STATUS_IO_DEVICE_ERROR = 0xC0000185;
        public static final int STATUS_FILE_CLOSED = 0xC0000128;
        public static final int STATUS_OBJECT_NAME_INVALID = 0xC0000033;
        public static final int STATUS_BUFFER_OVERFLOW = 0x80000005;
        public static final int STATUS_NOT_A_DIRECTORY = 0xC0000103;
        public static final int STATUS_SHARING_VIOLATION = 0xC0000043;
        public static final int STATUS_FILE_LOCK_CONFLICT = 0xC0000054;
        public static final int STATUS_LOCK_NOT_GRANTED = 0xC0000055;
        public static final int STATUS_RANGE_NOT_LOCKED = 0xC000007E;
        public static final int STATUS_BUFFER_TOO_SMALL = 0xC0000023;
        public static final int STATUS_FS_DRIVER_REQUIRED = 0xC000019C;

        //fs quota
        public static final int STATUS_NO_QUOTAS_FOR_ACCOUNT = 0x0000010D;
        public static final int STATUS_INVALID_QUOTA_LOWER = 0xC0000031;
        public static final int STATUS_DISK_QUOTA_EXCEEDED = 0xC0000802;
        public static final int STATUS_QUOTA_EXCEEDED = 0xC0000044;
        public static final int STATUS_REGISTRY_QUOTA_LIMIT = 0xC0000256;
        public static final int STATUS_NO_MORE_ENTRIES = 0x8000001A;

        //async
        public static final int STATUS_PENDING = 0x0103;
        public static final int STATUS_CANCELLED = 0xC0000120;
    }

    public static class SMBSetInfoLevel {
        public static final int SMB_POSIX_OPEN = 0x209;
        public static final int SMB_POSIX_UNLINK = 0x20a;
        public static final int SMB_SET_FILE_UNIX_HLINK = 0x203;
        public static final int SMB_SET_FILE_UNIX_LINK = 0x201;
        public static final int SMB_SET_FILE_BASIC_INFO = 0x101;
        public static final int SMB_SET_FILE_BASIC_INFO2 = 0x3ec;
        public static final int SMB_SET_FILE_UNIX_BASIC = 0x200;
        public static final int SMB_SET_FILE_EA = 2;
        public static final int SMB_SET_POSIX_ACL = 0x204;
        public static final int SMB_SET_FILE_END_OF_FILE_INFO2 = 0x3fc;
        public static final int SMB_SET_FILE_END_OF_FILE_INFO = 0x104;
    }

    //smb file attr
    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/6008aa8f-d2d8-4366-b775-b81aece05bb1
    public static final int FILE_ATTRIBUTE_READONLY = 0x0001;
    public static final int FILE_ATTRIBUTE_HIDDEN = 0x0002;
    public static final int FILE_ATTRIBUTE_SYSTEM = 0x0004;
    public static final int FILE_ATTRIBUTE_VOLUME = 0x0008;
    public static final int FILE_ATTRIBUTE_DIRECTORY = 0x0010;
    public static final int FILE_ATTRIBUTE_ARCHIVE = 0x0020;
    public static final int FILE_ATTRIBUTE_DEVICE = 0x0040;
    public static final int FILE_ATTRIBUTE_NORMAL = 0x0080;
    public static final int FILE_ATTRIBUTE_TEMPORARY = 0x0100;
    public static final int FILE_ATTRIBUTE_SPARSE = 0x0200;
    public static final int FILE_ATTRIBUTE_REPARSE_POINT = 0x0400;
    public static final int FILE_ATTRIBUTE_COMPRESSED = 0x0800;
    public static final int FILE_ATTRIBUTE_OFFLINE = 0x1000;
    public static final int FILE_ATTRIBUTE_NONINDEXED = 0x2000;
    public static final int FILE_ATTRIBUTE_ENCRYPTED = 0x4000;
    public static final int FILE_ATTRIBUTE_ALL_MASK = 0x7FFF;
    public static final int FILE_ATTRIBUTE_PIPE_ACCESS_MASK = 0x0012019F;
    public static final int MAX_CIFS_LOOKUP_BUFFER_SIZE = (16 * 1024);

    // NLM4 Stats
    // https://pubs.opengroup.org/onlinepubs/9629799/chap14.htm
    public static class NLM4Stats {
        public static final int NLM4_GRANTED = 0;
        public static final int NLM4_DENIED = 1;
        public static final int NLM4_DENIED_NOLOCKS = 2;
        public static final int NLM4_BLOCKED = 3;
        public static final int NLM4_DENIED_GRACE_PERIOD = 4;
        public static final int NLM4_DEADLCK = 5;
        public static final int NLM4_ROFS = 6;
        public static final int NLM4_STALE_FH = 7;
        public static final int NLM4_FBIG = 8;
        public static final int NLM4_FAILED = 9;
    }

    public static class NSMStats {
        public static final int STAT_SUCC = 0;
        public static final int STAT_FAIL = 1;
    }

    public static class ACLConstants {
        public static final String PROTO = "proto";
        public static final String S3_ID = "s3_id";
        public static final String UID = "uid";
        public static final String MASTER_GID = "master_gid";
        public static final String GIDS = "gids";
        //仅在nfs端判断目录是否具有cifs的删除子文件和子文件夹权限时使用
        public static final String NFS_DIR_SUB_DEL_ACL = "nfs_dir_sub_del";
        //不存在所要表示的权限位
        public static final String NOT_CONTAIN_ACL = "0";
        //存在所要表示的权限位
        public static final String CONTAIN_ACL = "1";
        public static final int START = 0b001;
        public static final String MERGE_META = "MERGE_META";
        public static final String FS_ID_MAX = "maxId";
        public static final String ADMIN_NAME = "_root";
    }

    //用于判断权限的类型是拒绝还是接受
    public static class SMB2ACEType {
        public static final byte ACCESS_ALLOWED_ACE_TYPE = 0x00;
        public static final byte ACCESS_DENIED_ACE_TYPE = 0x01;
        public static final byte SYSTEM_AUDIT_ACE_TYPE = 0x02;
        public static final byte SYSTEM_ALARM_ACE_TYPE = 0x03;
        public static final byte ACCESS_ALLOWED_COMPOUND_ACE_TYPE = 0x04;
        public static final byte ACCESS_ALLOWED_OBJECT_ACE_TYPE = 0x05;
        public static final byte ACCESS_DENIED_OBJECT_ACE_TYPE = 0x06;
        public static final byte SYSTEM_AUDIT_OBJECT_ACE_TYPE = 0x07;
        public static final byte SYSTEM_ALARM_OBJECT_ACE_TYPE = 0x08;
        public static final byte ACCESS_ALLOWED_CALLBACK_ACE_TYPE = 0x09;
        public static final byte ACCESS_DENIED_CALLBACK_ACE_TYPE = 0x0A;
        public static final byte ACCESS_ALLOWED_CALLBACK_OBJECT_ACE_TYPE = 0x0B;
        public static final byte ACCESS_DENIED_CALLBACK_OBJECT_ACE_TYPE = 0x0C;
        public static final byte SYSTEM_AUDIT_CALLBACK_ACE_TYPE = 0x0D;
        public static final byte SYSTEM_ALARM_CALLBACK_ACE_TYPE = 0x0E;
        public static final byte SYSTEM_AUDIT_CALLBACK_OBJECT_ACE_TYPE = 0x0F;
        public static final byte SYSTEM_ALARM_CALLBACK_OBJECT_ACE_TYPE = 0x10;
        public static final byte SYSTEM_MANDATORY_LABEL_ACE_TYPE = 0x11;
        public static final byte SYSTEM_RESOURCE_ATTRIBUTE_ACE_TYPE = 0x12;
        public static final byte SYSTEM_SCOPED_POLICY_ID_ACE_TYPE = 0x13;
    }

    //用于判断文件或目录权限应用的范围
    public static class SMB2ACEFlag {
        //容器(目录)会将自己的ACE继承给容器中的子目录或文件
        public static final short CONTAINER_INHERIT_ACE = 0X02;
        public static final short FAILED_ACCESS_ACE_FLAG = 0x80;
        //设置时表示当前ACE为用于继承的ACE，而不是控制当前文件或目录权限的有效ACE
        public static final short INHERIT_ONLY_ACE = 0x08;
        //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/180e7c76-12de-4d1b-8dfd-44667140336b
        //表示当前ACE是由继承机制从父目录获取，而非直接赋予的权限
        public static final short INHERITED_ACE = 0x10;
        //表示ACE可被当前对象继承，但不会传播到下一代
        public static final short NO_PROPAGATE_INHERIT_ACE = 0x04;
        //设置后文件可直接继承父目录的ACE，而子目录则将当前ACE视为INHERIT_ONLY_ACE
        public static final short OBJECT_INHERIT_ACE = 0x01;
        public static final short SUCCESSFUL_ACCESS_ACE_FLAG = 0x40;
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/7a53f60e-e730-4dfe-bbe9-b21b62eb790b
    //用于判断文件或目录本身的权限
    public static class SMB2ACCESS_MASK {
        public static final long CLEAR_GENERIC_ACCESS      = 0b0000_1111_1111_1111_1111_1111_1111_1111;
        public static final long CLEAR_SYNCHRONIZE_ACCESS  = 0b1111_1111_1110_1111_1111_1111_1111_1111;
        public static final long CLEAR_MAXIMUM_ALLOWED_ACCESS  = 0b1111_1101_1111_1111_1111_1111_1111_1111;
        public static final long CLEAR_ACCESS_SYSTEM_SECURITY_ACCESS  = 0b1111_1110_1111_1111_1111_1111_1111_1111;
        public static final long CLEAR_READ_ATTR_ACCESS       = 0b1111_1111_1111_1111_1111_1111_0111_1111;
        public static final long CLEAR_WRITE_OWNER_ACCESS     = 0b1111_1111_1111_0111_1111_1111_1111_1111;
        public static final long CLEAR_WRITE_DACL_ACCESS      = 0b1111_1111_1111_1011_1111_1111_1111_1111;
        public static final long CLEAR_READ_CONTROL_ACCESS    = 0b1111_1111_1111_1101_1111_1111_1111_1111;
        public static final long CLEAR_DELETE_ACCESS          = 0b1111_1111_1111_1110_1111_1111_1111_1111;

        //通用权利，使用时需转换为自定义的权限模板，即X位设置的权限
        //共32位，0-3位
        public static final long GENERIC_READ = 0x8000_0000L;
        public static final long GENERIC_WRITE = 0x4000_0000L;
        public static final long GENERIC_EXECUTE = 0x2000_0000L;
        public static final long GENERIC_ALL = 0x1000_0000L;
        //4-5位以及8-10位为预留位
        //6位为最大权限位
        public static final long MAXIMUM_ALLOWED = 0x0200_0000L;
        //7位为系统审计权限位sACL
        public static final long ACCESS_SYSTEM_SECURITY = 0x0100_0000L;

        //11位为等待文件的权限状态变更完毕
        public static final long SYNCHRONIZE = 0x0010_0000L;
        //12位允许修改对象的所有权，将所有者变更为其它用户或组
        public static final long WRITE_OWNER = 0x0008_0000L;
        //13位允许修改对象的自由访问控制列表，即调整其它用户对该对象的访问权限
        public static final long WRITE_DACL = 0x0004_0000L;
        //14位允许读取对象的安全描述符ACE
        public static final long READ_CONTROL = 0x0002_0000L;
        //15位允许删除对象
        public static final long DELETE = 0x0001_0000L;

        //以下是windows文件系统在特定位上的语义
        //16-22位暂无
        //23位写入属性，需|SYNCHRONIZE
        public static final long WRITE_ATTR = 0x0000_0100;

        //24位读取属性，需|SYNCHRONIZE
        public static final long READ_ATTR = 0x0000_0080;

        //25位删除子文件夹和文件，需|SYNCHRONIZE
        public static final long DELETE_SUB_FILE_AND_DIR = 0x0000_0040;

        //26位遍历文件夹/执行文件，需|SYNCHRONIZE
        public static final long LIST_DIR_OR_EXEC_FILE = 0x0000_0020;

        //27位写入扩展属性，需|SYNCHRONIZE
        public static final long WRITE_X_ATTR = 0x0000_0010;

        //28位读取扩展属性，需|SYNCHRONIZE
        public static final long READ_X_ATTR = 0x0000_0008;

        //29位创建文件夹/附加数据，需|SYNCHRONIZE
        public static final long MKDIR_OR_APPEND_DATA = 0x0000_0004;

        //30位创建文件/写数据，需|SYNCHRONIZE
        public static final long CREATE_OR_WRITE_DATA = 0x0000_0002;

        //31位列出文件夹/读取数据，需|SYNCHRONIZE
        public static final long LOOKUP_DIR_OR_READ_DATA = 0x0000_0001;

        //ugo: read，源码 linux cifspdu.h
        public static final long SET_FILE_READ_RIGHTS =
                (LOOKUP_DIR_OR_READ_DATA
                        | READ_X_ATTR | WRITE_X_ATTR | READ_ATTR | WRITE_ATTR
//                | DELETE
                        | READ_CONTROL | WRITE_DACL | WRITE_OWNER | SYNCHRONIZE);

        //ugo: write
        public static final long SET_FILE_WRITE_RIGHTS =
                (CREATE_OR_WRITE_DATA | MKDIR_OR_APPEND_DATA | DELETE_SUB_FILE_AND_DIR
                        | READ_X_ATTR | WRITE_X_ATTR | WRITE_ATTR | READ_ATTR
                        | DELETE
                        | READ_CONTROL | WRITE_DACL | WRITE_OWNER | SYNCHRONIZE);

        //ugo: exec
        public static final long SET_FILE_EXEC_RIGHTS =
                ( LIST_DIR_OR_EXEC_FILE
                        | READ_X_ATTR | WRITE_X_ATTR | WRITE_ATTR | READ_ATTR
//                | DELETE
                        | READ_CONTROL | WRITE_DACL | WRITE_OWNER | SYNCHRONIZE);

        public static final long SET_FILE_ALL_RIGHTS = SET_FILE_READ_RIGHTS
                | SET_FILE_WRITE_RIGHTS
                | SET_FILE_EXEC_RIGHTS;

        public static final long SET_GENERIC_READ =
                (LOOKUP_DIR_OR_READ_DATA | READ_X_ATTR | READ_ATTR | READ_CONTROL | SYNCHRONIZE);

        public static final long SET_GENERIC_WRITE =
                (CREATE_OR_WRITE_DATA | MKDIR_OR_APPEND_DATA
                        | WRITE_X_ATTR | WRITE_ATTR | READ_CONTROL | SYNCHRONIZE);

        public static final long SET_GENERIC_EXEC =
                (LIST_DIR_OR_EXEC_FILE
                        | READ_X_ATTR | READ_ATTR
                        | READ_CONTROL | SYNCHRONIZE);

        public static final long SET_GENERIC_ALL = (SET_GENERIC_READ | SET_GENERIC_WRITE | SET_GENERIC_EXEC);

    }

    public static class ProtoFlag {
        public static final int NFSV3_START = 0b001;
        public static final int CIFS_START  = 0b010;
        public static final int FTP_START   = 0b100;
    }

    public static class CIFSJudge {
        //int长度共4字节，最长可以表示16位
        public static final int IS_DIR            = 0b00001;
        public static final int IS_FILE           = 0b00010;
        public static final int IS_NFSv3_REQUEST  = 0b00100;
        public static final int IS_CIFS_REQUEST   = 0b01000;
        public static final int IS_S3_REQUEST     = 0b10000;
    }

    public static class FSConfig {
        public static String FS_CONFIG = "fs_config";

        //445  cifs端口
        public static String CIFS_PORT = "cifs_port";

        //2049  nfs端口
        public static String NFS_PORT = "nfs_port";

        //20048  nfs挂载端口
        public static String NFS_MOUNT_PORT = "nfs_mount_port";

        //4045  nlm端口
        public static String NLM_PORT = "nlm_port";

        //32767  nsm端口
        public static String NSM_PORT = "nsm_port";

        //875  配额端口
        public static String NFS_QUOTA_PORT = "nfs_quota_port";

        //21 ftp端口
        public static String FTP_CONTROL_PORT = "ftp_control_port";

        //990 ftps端口
        public static String FTPS_CONTROL_PORT = "ftps_control_port";
    }
}
