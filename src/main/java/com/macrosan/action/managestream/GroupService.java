package com.macrosan.action.managestream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.socketmsg.ListMapResMsg;
import com.macrosan.message.socketmsg.MapResMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.GroupInfoResponse;
import com.macrosan.message.xmlmsg.GroupInfosResponse;
import com.macrosan.message.xmlmsg.UserInfosResponse;
import com.macrosan.message.xmlmsg.section.GroupInfo;
import com.macrosan.message.xmlmsg.section.GroupInfos;
import com.macrosan.message.xmlmsg.section.UserInfo;
import com.macrosan.message.xmlmsg.section.UserInfos;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.macrosan.constants.IAMConstants.*;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.message.consturct.SocketReqMsgBuilder.*;
import static com.macrosan.utils.functional.exception.Sleep.sleep;
import static com.macrosan.utils.regex.PatternConst.GROUP_NAME_PATTERN;
import static com.macrosan.utils.regex.PatternConst.USER_NAME_PATTERN;


/**
 * GroupService
 * 非线程安全单例模式
 *
 * @author shilinyong
 * @date 2019/08/02
 */
public class GroupService extends BaseService {

    private static final Logger logger = LogManager.getLogger(GroupService.class.getName());

    private static GroupService instance = null;

    private GroupService() {
        super();
    }

    private UserService userService;

    /**
     * 每一个Service都必须提供一个getInstance方法
     */
    public static GroupService getInstance() {
        if (instance == null) {
            instance = new GroupService();
        }
        return instance;
    }

    private GroupInfo setGroupInfoFromIAM(Map<String, String> res) {
        GroupInfo groupInfo = new GroupInfo();
        groupInfo.setGroupId(res.get("groupId"));
        groupInfo.setGroupName(res.get("groupName"));
        groupInfo.setGroupNickName(res.getOrDefault("groupNickname", ""));
        groupInfo.setRemark(res.getOrDefault("remark",""));
        String addTime =  res.get("createTime");
        addTime = addTime.contains(":")?addTime: MsDateUtils.stampToSimpleDate(addTime);
        groupInfo.setCreateTime(addTime);
        groupInfo.setGroupMRN(res.get("mrn"));

        return groupInfo;
    }

    /**
     * 创建用户组
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg createGroup(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String groupName = paramMap.get(IAM_GROUP_NAME);
        String groupNickName = paramMap.getOrDefault("GroupNickName","");
        String remark = paramMap.getOrDefault("Remark","");
        MsAclUtils.checkIfAnonymous(accountId);
        //用户组名称校验
        if (StringUtils.isEmpty(groupName) || !GROUP_NAME_PATTERN.matcher(groupName).matches()) {
            throw new MsException(ErrorNo.GROUP_NAME_INPUT_ERR,
                    "createGroup failed, group name input error, group_name: " + groupName + ".");
        }

        //用户组昵称长度不能大于64
        if (groupNickName.length() > 64) {
            throw new MsException(ErrorNo.INVALID_LENGTH,
                    "Input length is over 64.");
        }

        //用户组备注长度不能大于128
        if (remark.length() > 128) {
            throw new MsException(ErrorNo.INVALID_LENGTH,
                    "Input length is over 128.");
        }
        GroupInfoResponse groupInfoResponse = new GroupInfoResponse();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildCreateGroupMsg(paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                groupInfoResponse.setGroupInfo(setGroupInfoFromIAM(res));
                logger.debug("Create group successfully,groupName: {}", groupName);
                return new ResponseMsg().setData(groupInfoResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.GROUP_EXISTED) {
                throw new MsException(ErrorNo.GROUP_EXISTED, "The group already exits.");
            } else if (code == ErrorNo.TOO_MANY_GROUP) {
                throw new MsException(ErrorNo.TOO_MANY_GROUP, "The number of group has reached upper limit.");
            } else if (code == ErrorNo.MOSS_FAIL){
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Create group failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 删除用户组
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg deleteGroup(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String groupName = paramMap.get(IAM_GROUP_NAME);
        MsAclUtils.checkIfAnonymous(accountId);
        //用户组名称校验
        if (StringUtils.isEmpty(groupName) || !GROUP_NAME_PATTERN.matcher(groupName).matches()) {
            throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
        }
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildDeleteGroupMsg(accountId, groupName);
            addSyncFlagParam(msg, paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                logger.debug("Delete group successfully,groupName: {}", groupName);
                return new ResponseMsg();
            } else if (code == ErrorNo.GROUP_NOT_EXISTS) {
                throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
            } else if (code == ErrorNo.GROUP_HAS_USERS_ERROR) {
                throw new MsException(ErrorNo.GROUP_HAS_USERS_ERROR, "The group has groups, please remove groups first.");
            } else if (code == ErrorNo.GROUP_ATTACH_POLICY) {
                throw new MsException(ErrorNo.GROUP_ATTACH_POLICY, "The group has attached policy, please detach policy first.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Delete group failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 更新用户组
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg updateGroup(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String groupName = paramMap.get(IAM_GROUP_NAME);
        String newGroupNickName = paramMap.getOrDefault("NewGroupNickName","");
        String newRemark = paramMap.getOrDefault("NewRemark","");
        MsAclUtils.checkIfAnonymous(accountId);

        //用户组名称校验
        if (StringUtils.isEmpty(groupName) || !GROUP_NAME_PATTERN.matcher(groupName).matches()) {
            throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
        }

        //用户组昵称长度不能大于64
        if (newGroupNickName.length() > 64) {
            throw new MsException(ErrorNo.INVALID_LENGTH,
                    "Input length is over 64.");
        }

        //用户组备注长度不能大于128
        if (newRemark.length() > 128) {
            throw new MsException(ErrorNo.INVALID_LENGTH,
                    "Input length is over 128.");
        }

        GroupInfoResponse groupInfoResponse = new GroupInfoResponse();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildUpdateGroupMsg(accountId, groupName, newGroupNickName, newRemark);
            addSyncFlagParam(msg, paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                groupInfoResponse.setGroupInfo(setGroupInfoFromIAM(res));
                return new ResponseMsg().setData(groupInfoResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.GROUP_NOT_EXISTS) {
                throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Update group failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 查询用户组列表
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg listGroups(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);

        GroupInfosResponse groupInfosResponse = new GroupInfosResponse();
        GroupInfos groupInfos = new GroupInfos();
        List<GroupInfo> groupList = new ArrayList<>();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildListGroupsMsg(accountId);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                for (Map<String, String> re : res) {
                    groupList.add(setGroupInfoFromIAM(re));
                }
                groupInfos.setGroupInfo(groupList);
                groupInfosResponse.setGroupInfos(groupInfos);
                return new ResponseMsg().setData(groupInfosResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List groups failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 查询特定用户组信息
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg getGroup(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String groupName = paramMap.get(IAM_GROUP_NAME);
        MsAclUtils.checkIfAnonymous(accountId);

        //用户组名称校验
        if (StringUtils.isEmpty(groupName) || !GROUP_NAME_PATTERN.matcher(groupName).matches()) {
            throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
        }

        GroupInfoResponse groupInfoResponse = new GroupInfoResponse();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildGetGroupMsg(accountId, groupName);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                groupInfoResponse.setGroupInfo(setGroupInfoFromIAM(res));
                return new ResponseMsg().setData(groupInfoResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.GROUP_NOT_EXISTS) {
                throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get group failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 批量添加用户到用户组
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg addUsersToGroups(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String userNames = paramMap.get("UserNames"); //用戶名以逗號間隔
        String groupNames = paramMap.get("GroupNames"); //組名以逗號間隔
        MsAclUtils.checkIfAnonymous(accountId);

        List<String> userList = Arrays.asList(userNames.split(","));
        List<String> groupList = Arrays.asList(groupNames.split(","));

        userList.forEach(user -> {
            if (StringUtils.isEmpty(user) || !USER_NAME_PATTERN.matcher(user).matches()){
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            }
        });
        groupList.forEach(group -> {
            if (StringUtils.isEmpty(group) || !GROUP_NAME_PATTERN.matcher(group).matches()){
                throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
            }
        });
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildAddUsersToGroupsMsg(accountId, userNames, groupNames);
            addSyncFlagParam(msg, paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                return new ResponseMsg();
            } else if (code == ErrorNo.USER_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            } else if (code == ErrorNo.GROUP_NOT_EXISTS) {
                throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
            } else if (code == ErrorNo.GROUP_HAD_USER) {
                throw new MsException(ErrorNo.GROUP_HAD_USER, "The group has already included the user.");
            } else if (code == ErrorNo.TOO_MANY_GROUP_IN_USER) {
                throw new MsException(ErrorNo.TOO_MANY_GROUP_IN_USER, "The number of group for the user has reached the upper limit.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Add users to groups failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 将用户从用户组中移除
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg removeUserFromGroup(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.get(IAM_USER_NAME);
        String groupName = paramMap.get(IAM_GROUP_NAME);
        MsAclUtils.checkIfAnonymous(accountId);
        if (StringUtils.isEmpty(userName) || !USER_NAME_PATTERN.matcher(userName).matches()){
            throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
        }
        if (StringUtils.isEmpty(groupName) || !GROUP_NAME_PATTERN.matcher(groupName).matches()){
            throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
        }
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildRemoveUserFromGroupMsg(accountId, userName, groupName);
            addSyncFlagParam(msg, paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                return new ResponseMsg();
            } else if (code == ErrorNo.USER_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            } else if (code == ErrorNo.GROUP_NOT_EXISTS) {
                throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
            } else if (code == ErrorNo.NO_SUCH_USER_IN_GROUP) {
                throw new MsException(ErrorNo.NO_SUCH_USER_IN_GROUP, "No such user in the group.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Remove user from group failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 列舉組中用戶
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg listUsersForGroup(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String groupName = paramMap.get(IAM_GROUP_NAME);
        MsAclUtils.checkIfAnonymous(accountId);
        if (StringUtils.isEmpty(groupName) || !GROUP_NAME_PATTERN.matcher(groupName).matches()){
            throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
        }
        UserInfosResponse userInfosResponse = new UserInfosResponse();
        UserInfos userInfos = new UserInfos();
        List<UserInfo> userList = new ArrayList<>();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildListUsersForGroupMsg(accountId, groupName);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                for (Map<String, String> re : res) {
                    userList.add(UserService.setUserInfoFromIAM(re));
                }
                userInfos.setUserInfo(userList);
                userInfosResponse.setUserInfos(userInfos);
                return new ResponseMsg().setData(userInfosResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.GROUP_NOT_EXISTS) {
                throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List users for group failed.");
            }
            sleep(10000);
        }
    }

    /**
     * 列舉用户所屬組
     *
     * @param paramMap 请求参数
     * @return 相应返回码
     */
    public ResponseMsg listGroupsForUser(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.get(IAM_USER_NAME);
        MsAclUtils.checkIfAnonymous(accountId);
        if (StringUtils.isEmpty(userName) || !USER_NAME_PATTERN.matcher(userName).matches()){
            throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
        }
        GroupInfosResponse groupInfosResponse = new GroupInfosResponse();
        GroupInfos groupInfos = new GroupInfos();
        List<GroupInfo> groupList = new ArrayList<>();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildListGroupsForUserMsg(accountId, userName);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                for (Map<String, String> re : res) {
                    groupList.add(setGroupInfoFromIAM(re));
                }
                groupInfos.setGroupInfo(groupList);
                groupInfosResponse.setGroupInfos(groupInfos);
                return new ResponseMsg().setData(groupInfosResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.USER_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List groups for user failed.");
            }
            sleep(10000);
        }
    }
}
