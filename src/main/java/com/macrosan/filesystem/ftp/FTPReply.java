package com.macrosan.filesystem.ftp;

public enum FTPReply {

    WELCOME(220, "Welcome to FTP Server"),
    GOODBYE(200, "Goodbye."),
    UNKNOWN_COMMAND(502, "Command not implemented"),

    // common
    COMMAND_OKAY(200, "Command okay."),
    COMMAND_OK(250, "Command okay."),
    SYNTAX_ERROR(501, "Syntax error."),

    // 数据连接常用命令
    OPENING_DATA_CONNECTION(150, "Opening data connection."),
    CANNOT_OPEN_DATA_CONNECTION(425, "Cannot open data connection."),
    DATA_CONNECTION_ERROR(426, "Data connection error."),

    CLOSING_DATA_CONNECTION(226, "Closing data connection."),
    TRANSFER_COMPLETE(226, "Transfer complete."),


    // AUTH
    SERVICE_UNAVAILABLE(431, "Service is unavailable."),
    AUTH_OKAY(234, "AUTH command okay; starting SSL connection."),

    // USER
    LOGIN_SUCCESSFUL(230, "Login successful."),
    INVALID_USER_NAME(530, "Invalid user name."),
    ANONYMOUS_NOT_ALLOWED(530, "Anonymous connection is not allowed."),
    USER_MAX_ANONYMOUS_LOGIN_LIMIT(421, "Maximum anonymous login limit has been reached."),
    USER_MAX_LOGIN_LIMIT(421, "Maximum login limit has been reached."),
    GUEST_LOGIN_OK(331, "Guest login okay, send your complete e-mail address as password."),
    USER_NAME_OK_NEED_PASSWORD(331, "User name okay, need password."),

    // PASS
    NEED_USER_FIRST(503, "Login with USER first."),
    ALREADY_LOGGED_IN(202, "Already logged-in."),
    PASS_MAX_ANONYMOUS_LOGIN_LIMIT(421, "Maximum anonymous login limit has been reached."),
    PASS_MAX_LOGIN_LIMIT(421, "Maximum login limit has been reached."),
    AUTHENTICATION_FAILED(530, "Authentication failed."),
    USER_LOGGED_IN(230, "User logged in, proceed."),

    //CWD
    FAIL_CWD(550, "Failed to change directory."),

    // TYPE
    COMMAND_NOT_IMPLEMENTED(501, "Command not implemented."),

    // STRU
    STR_COMMAND_NOT_IMPLEMENTED(504, "Command not implemented."),

    // PWD
    CURRENT_DIRECTORY(257, "\"%s\" "),

    // OPTS
    EXECUTION_FAILED(500, "Execution failed."),
    SYNTAX_ERROR_PARAMETERS(501, "Syntax error in parameters or arguments."),
    COMMAND_OPTS_NOT_IMPLEMENTED(502, "Command OPTS not implemented for ..."),

    // LIST and MLSD
    FILE_LISTING_FAILED(551, "File listing failed."),

    // MLST
    NOT_A_VALID_PATHNAME(501, "Not a valid pathname."),

    // PASV
    ENTER_PASSIVE_MODE(227, "Entering passive mode %s"),

    // SYST
    SYSTEM_TYPE(215, "UNIX Type: MOSS FTP Server."),

    // STOR and APPE
    ERROR_ON_OUTPUT_FILE(551, "Error on output file."),
    EXCEEDED_QUOTA(552, "Exceeded quota."),
    FILE_NAME_NOT_ALLOWED(553, "File name not allowed."),

    // DELE
    CANT_DELETE_FILE(450, "Can't delete file."),
    NO_PERMISSION_TO_DELETE(450, "No permission to delete."),

    // RETR and APPE
    NO_SUCH_FILE_OR_DIRECTORY(550, "No such file or directory."),
    NOT_A_PLAIN_FILE(550, "Not a plain file."),
    PERMISSION_DENIED(550, "Permission denied."),
    ERROR_ON_INPUT_FILE(551, "Error on input file."),


    // MKD
    NOT_A_VALID_FILE(550, "Not a valid file."),
    ALREADY_EXISTS(550, "Already exists."),
    NO_PERMISSION(550, "No permission."),
    DIRECTORY_CREATED(250, "Directory created."),
    CANNOT_CREATE_DIRECTORY(550, "Cannot create directory."),


    // RMD
    NOT_A_VALID_DIRECTORY(550, "Not a valid directory."),
    DIRECTORY_REMOVED(250, "Directory removed."),
    CANNOT_REMOVE_DIRECTORY(550, "Cannot remove directory."),


    // RNFR
    FILE_UNAVAILABLE(550, "File unavailable."),
    REQUESTED_FILE_ACTION_PENDING(350, "Requested file action pending further information."),


    // RNTO
    CANNOT_FIND_FILE_TO_RENAME(503, "Cannot find the file which has to be renamed."),
    INVALID_FILE_NAME(553, "Not a valid file name."),
    FILE_ACTION_OK_RENAMED(250, "Requested file action okay, file renamed."),
    CANNOT_RENAME_FILE(553, "Cannot rename file."),
    NO_PERMISSION_RNTO(553, "No permission."),

    // STOU
    UNIQUE_FILE_NAME_ERROR(550, "Unique file name error."),
    FILE_OPENING_DATA_CONNECTION(150, "FILE: %s"),

    // REST
    INVALID_NUMBER(501, "Not a valid number."),
    NEGATIVE_MARKER(501, "Marker cannot be negative."),
    RESTART_POSITION(350, "Restarting at %s. Send STORE or RETRIEVE to initiate transfer."),

    //MFMT
    REQUESTED_ACTION_NOT_TAKEN(550, "Requested action not taken"),

    //SIZE
    SIZE(213, "%s");

    public int code;
    public String msg;
    private static final String CRLF = "\r\n";

    FTPReply(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    // 增加一个方法，用于生成动态消息
    public String reply(String... params) {
        String formattedMessage = String.format(msg, (Object[]) params);
        return code + " " + formattedMessage + CRLF;
    }

    public String reply() {
        return code + " " + msg + CRLF;
    }

    public static String notReply() {
        return "";
    }
}
