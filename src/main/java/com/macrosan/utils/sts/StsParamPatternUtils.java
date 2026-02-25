package com.macrosan.utils.sts;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.IamRedisConnPool;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.regex.Pattern;
import com.macrosan.utils.regex.PatternConst;
import org.apache.commons.lang3.StringUtils;

import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.SysConstants.REDIS_IAM_INDEX;
import static com.macrosan.utils.regex.PatternConst.POLICY_NAME_PATTERN;
import static com.macrosan.utils.sts.RoleUtils.MAX_SESSION_DURATION;

/**
 * @Description: TODO
 * @Author wanhao
 * @Date 2023/8/4 0004 上午 11:43
 */
public class StsParamPatternUtils {
    public static final long DEFAULT_SESSION_DURATION_SECONDS = 3600;
    public static final long MIN_SESSION_DURATION_SECONDS = 3600;
    public static final long MAX_SESSION_DURATION_SECONDS = 43200;


    public static final long DEFAULT_DURATION_SECONDS = 3600;
    public static final long MAX_DURATION_SECONDS = 43200;
    public static final long MIN_DURATION_SECONDS = 900;

    public static final long MIN_PATH_LENGTH = 1;
    public static final long MAX_PATH_LENGTH = 512;

    public static final long MIN_ROLE_SESSION_NAME_LENGTH = 2;
    public static final long MAX_ROLE_SESSION_NAME_LENGTH = 64;

    public static final long MIN_ROLE_NAME_LENGTH = 1;
    public static final long MAX_ROLE_NAME_LENGTH = 64;

    private static final Pattern ROLE_SESSION_NAME_PATTERN = Pattern.compile("[\\w+=,.@-]*");
    public static final Pattern ROLE_NAME_PATTERN = Pattern.compile("[\\w+=,.@-]*");
    public static final Pattern MARKER__PATTERN = Pattern.compile("[\\u0020-\\u00FF]+");

    private static final IamRedisConnPool IAM_POOL = IamRedisConnPool.getInstance();

    public static void checkRoleSessionName(String roleSessionName) {
        if (roleSessionName == null) {
            throw new MsException(MISSING_ROLE_SESSION_NAME, "Missing required parameter for this request: 'RoleSessionName'");
        }
        if (roleSessionName.length() < MIN_ROLE_SESSION_NAME_LENGTH || roleSessionName.length() > MAX_ROLE_SESSION_NAME_LENGTH) {
            throw new MsException(INVALID_ROLE_SESSION_NAME, "The input 'RoleSessionName' is invalid.");
        }
        boolean matches = ROLE_SESSION_NAME_PATTERN.matcher(roleSessionName).matches();
        if (!matches) {
            throw new MsException(INVALID_ROLE_SESSION_NAME, "The input 'RoleSessionName' is invalid.");
        }
    }

    public static void checkRoleDescription(String description) {
        if (description != null && description.length() > 1000) {
            throw new MsException(INVALID_ROLE_DESCRIPTION, "The input 'RoleDescription' is invalid.");
        }
    }

    public static void checkMaxSessionDuration(String durationStr) {
        if (!StringUtils.isNumeric(durationStr)) {
            throw new MsException(INVALID_MAX_SESSION_DURATIONS, "The input 'MaxSessionDuration' is invalid.");
        }
        long duration = Long.parseLong(durationStr);
        String systemMaxSessionDurationStr = IAM_POOL.getCommand(REDIS_IAM_INDEX).get(MAX_SESSION_DURATION);
        if (StringUtils.isNumeric(systemMaxSessionDurationStr)) {
            long systemMaxSessionDuration = Long.parseLong(systemMaxSessionDurationStr);
            if (duration < MIN_SESSION_DURATION_SECONDS || duration > systemMaxSessionDuration) {
                throw new MsException(INVALID_MAX_SESSION_DURATIONS, "MaxSessionDuration must be between 3600 and " + systemMaxSessionDuration + " seconds.");
            }
        } else {
            if (duration < MIN_SESSION_DURATION_SECONDS || duration > MAX_SESSION_DURATION_SECONDS) {
                throw new MsException(INVALID_MAX_SESSION_DURATIONS, "MaxSessionDuration must be between 3600 and 43200 seconds.");
            }
        }
    }

    public static void checkPath(String path) {
        if (path == null) {
            return;
        }
        if (path.length() < MIN_PATH_LENGTH || path.length() > MAX_PATH_LENGTH) {
            throw new MsException(INVALID_PATH, "The input 'Path' is invalid.");
        }
    }

    public static void checkRoleNameIsNull(String roleName){
        if (roleName == null) {
            throw new MsException(ROLE_NOT_EXISTS, "Missing required parameter for this request: 'RoleName'");
        }
        if (roleName.length() < MIN_ROLE_NAME_LENGTH || roleName.length() > MAX_ROLE_NAME_LENGTH) {
            throw new MsException(ROLE_NOT_EXISTS, "The input 'RoleName' is invalid.");
        }
        boolean matches = ROLE_NAME_PATTERN.matcher(roleName).matches();
        if (!matches) {
            throw new MsException(ROLE_NOT_EXISTS, "The input 'RoleName' is invalid.");
        }
    }

    public static void checkRoleName(String roleName) {
        if (roleName.length() < MIN_ROLE_NAME_LENGTH || roleName.length() > MAX_ROLE_NAME_LENGTH) {
            throw new MsException(INVALID_ROLE_NAME, "The input 'RoleName' is invalid.");
        }
        boolean matches = ROLE_NAME_PATTERN.matcher(roleName).matches();
        if (!matches) {
            throw new MsException(INVALID_ROLE_NAME, "The input 'RoleName' is invalid.");
        }
    }

    public static void checkAssumeDurationSeconds(String durationSecondsStr) {
        if (!StringUtils.isNumeric(durationSecondsStr)) {
            throw new MsException(INVALID_DURATION_SECONDS, "Invalid value for parameter 'durationSeconds'.");
        }
        long durationSeconds = Long.parseLong(durationSecondsStr);
        String systemMaxSessionDurationStr = IAM_POOL.getCommand(REDIS_IAM_INDEX).get(MAX_SESSION_DURATION);
        if (StringUtils.isNumeric(systemMaxSessionDurationStr)) {
            long systemMaxSessionDuration = Long.parseLong(systemMaxSessionDurationStr);
            if (durationSeconds > systemMaxSessionDuration || durationSeconds < MIN_DURATION_SECONDS) {
                throw new MsException(INVALID_DURATION_SECONDS, "DurationSeconds must be between 900 and " + systemMaxSessionDuration + " seconds.");
            }
        } else {
            if (durationSeconds > MAX_DURATION_SECONDS || durationSeconds < MIN_DURATION_SECONDS) {
                throw new MsException(INVALID_DURATION_SECONDS, "DurationSeconds must be between 900 and 43200 seconds.");
            }
        }
    }

    public static boolean checkUpdateMaxSessionDuration(String durationStr) {
        if (!StringUtils.isNumeric(durationStr)) {
            throw new MsException(INVALID_MAX_SESSION_DURATIONS, "The input 'MaxSessionDuration' is invalid.");
        }
        long duration = Long.parseLong(durationStr);
        String systemMaxSessionDurationStr = IAM_POOL.getCommand(REDIS_IAM_INDEX).get(MAX_SESSION_DURATION);
        if (StringUtils.isNumeric(systemMaxSessionDurationStr)) {
            long systemMaxSessionDuration = Long.parseLong(systemMaxSessionDurationStr);
            if (duration < MIN_SESSION_DURATION_SECONDS || duration > systemMaxSessionDuration) {
                throw new MsException(INVALID_MAX_SESSION_DURATIONS, "The input 'MaxSessionDuration' is invalid.");
            }
        } else {
            if (duration < MIN_SESSION_DURATION_SECONDS || duration > MAX_SESSION_DURATION_SECONDS) {
                throw new MsException(INVALID_MAX_SESSION_DURATIONS, "The input 'MaxSessionDuration' is invalid.");
            }
        }
        return true;
    }

    public static void checkMaxItems(String maxItems) {
        if (!PatternConst.LIST_PARTS_PATTERN.matcher(maxItems).matches()) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "MaxItems param error.");
        }
        int max = Integer.parseInt(maxItems);
        if (max > 1000 || max <= 0) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "MaxItems param error, must be int in [1, 1000]");
        }
    }

    public static void checkPolicyNameIsNull(String policyName) {
        if (policyName == null) {
            throw new MsException(POLICY_NOT_EXISTS_ROLE, "Missing required parameter for this request: 'PolicyName'");
        }
        if (policyName.length() < 1 || policyName.length() > 128) {
            throw new MsException(POLICY_NOT_EXISTS_ROLE, "The input 'PolicyName' is invalid.");
        }
        if (!POLICY_NAME_PATTERN.matcher(policyName).matches()) {
            throw new MsException(POLICY_NOT_EXISTS_ROLE, "The input 'PolicyName' is invalid.");
        }
    }

    public static void checkPolicyName(String policyName) {
        if (policyName == null) {
            throw new MsException(INVALID_POLICY_NAME, "Missing required parameter for this request: 'PolicyName'");
        }
        if (policyName.length() < 1 || policyName.length() > 128) {
            throw new MsException(INVALID_POLICY_NAME, "The input 'PolicyName' is invalid.");
        }
        if (!POLICY_NAME_PATTERN.matcher(policyName).matches()) {
            throw new MsException(INVALID_POLICY_NAME, "The input 'PolicyName' is invalid.");
        }
    }

    public static String checkAndGetMarker(String marker) {
        if (marker == null) {
            return "";
        }
        if (marker.length() < 1) {
            throw new MsException(INVALID_MARKER, "The input 'Marker' is invalid.");
        }
        if (!MARKER__PATTERN.matcher(marker).matches()) {
            throw new MsException(INVALID_MARKER, "The input 'Marker' is invalid.");
        }
        return marker;
    }

}
