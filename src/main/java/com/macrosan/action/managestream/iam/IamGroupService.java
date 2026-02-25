package com.macrosan.action.managestream.iam;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.socketmsg.ListMapResMsg;
import com.macrosan.message.socketmsg.MapResMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.iam.*;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.regex.PatternConst;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.io.UnsupportedEncodingException;
import java.util.*;

import static com.macrosan.constants.IAMConstants.*;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.MSG_TYPE_CREATE_GROUP;
import static com.macrosan.message.consturct.SocketReqMsgBuilder.*;
import static com.macrosan.utils.functional.exception.Sleep.sleep;
import static com.macrosan.utils.regex.PatternConst.GROUP_NAME_PATTERN;
import static com.macrosan.utils.regex.PatternConst.USER_NAME_PATTERN;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/7/28 19:29
 * @Email :   joshua_zhang_email@163.com
 */

@Log4j2
public class IamGroupService extends BaseService {

    private static IamGroupService instance = null;

    private IamGroupService() {
        super();
    }

    public static IamGroupService getInstance() {
        if (instance == null) {
            instance = new IamGroupService();
        }
        return instance;
    }

    public ResponseMsg createGroup(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        log.debug("createGroup {}", paramMap);
        String accountId = paramMap.get(USER_ID);
        String groupName = paramMap.get(IAM_GROUP_NAME);
        MsAclUtils.checkIfAnonymous(accountId);

        if (StringUtils.isEmpty(groupName) || !GROUP_NAME_PATTERN.matcher(groupName).matches()) {
            throw new MsException(ErrorNo.GROUP_NAME_INPUT_ERR,
                    "createGroup failed, group name input error, group_name: " + groupName + ".");
        }

        CreateGroupResponse createGroupResponse = new CreateGroupResponse();
        SocketReqMsg msg = new SocketReqMsg(MSG_TYPE_CREATE_GROUP, 0);
        msg.put("accountId", accountId);
        msg.put("groupName", groupName);
        msg.put("groupNickName", "");
        msg.put("remark", "");
        String createTime = paramMap.getOrDefault(IAM_CREATE_TIME, "");
        msg.put("createTime", createTime);
        CreateGroupResponse.CreateGroupResult result = new CreateGroupResponse.CreateGroupResult();
        createGroupResponse.setCreateGroupResult(result).setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));

        for (int tryTime = 10; ; tryTime -= 1) {
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                result.setGroup(setGroupInfoFromIAM(res));
                log.debug("Iam create group successfully,groupName: {}", groupName);
                return new ResponseMsg().setData(createGroupResponse).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.GROUP_EXISTED) {
                throw new MsException(ErrorNo.GROUP_EXISTED, "The group already exits.");
            } else if (code == ErrorNo.TOO_MANY_GROUP) {
                throw new MsException(ErrorNo.TOO_MANY_GROUP, "The number of group has reached upper limit.");
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Create group failed.");
            }
            sleep(10000);
        }

    }

    public ResponseMsg deleteGroup(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String groupName = paramMap.get(IAM_GROUP_NAME);
        MsAclUtils.checkIfAnonymous(accountId);
        //用户组名称校验
        if (StringUtils.isEmpty(groupName) || !GROUP_NAME_PATTERN.matcher(groupName).matches()) {
            throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
        }

        DeleteGroupResponse response = new DeleteGroupResponse()
                .setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));
        SocketReqMsg msg = buildDeleteGroupMsg(accountId, groupName);
        for (int tryTime = 10; ; tryTime -= 1) {

            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                log.debug("Iam delete group successfully,groupName: {}", groupName);
                return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
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

    public ResponseMsg updateGroup1(UnifiedMap<String, String> paramMap) throws UnsupportedEncodingException {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String groupName = paramMap.get(IAM_GROUP_NAME);
        String newGroupName = paramMap.get(AWS_IAM_NEW_GROUP_NAME);
        MsAclUtils.checkIfAnonymous(accountId);

        //用户组名称校验
        if (StringUtils.isEmpty(groupName) || !GROUP_NAME_PATTERN.matcher(groupName).matches()) {
            throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
        }

        if (newGroupName.length() > 64) {
            throw new MsException(ErrorNo.INVALID_LENGTH,
                    "Input length is over 64.");
        }

        UpdateGroupResponse response = new UpdateGroupResponse().setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildUpdateGroupMsg(accountId, groupName, newGroupName, "");
            addSyncFlagParam(msg, paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.GROUP_NOT_EXISTS) {
                throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Update group failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg listGroups(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);

        String marker = paramMap.getOrDefault("Marker", "");
        String maxStr = paramMap.getOrDefault("MaxItems", "1000");

        if (!PatternConst.LIST_PARTS_PATTERN.matcher(maxStr).matches()) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "MaxItems param error.");
        }
        int max = Integer.parseInt(maxStr);
        if (max > 1000 || max <= 0) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "MaxItems param error, must be int in [1, 1000]");
        }

        ListGroupsResponse.ListGroupsResult result = new ListGroupsResponse.ListGroupsResult();
        ListGroupsResponse response = new ListGroupsResponse()
                .setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));
        ListGroupsResponse.Groups groups = new ListGroupsResponse.Groups();
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildListGroupsMsg(accountId);
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                TreeMap<String, GroupMember> members = new TreeMap<>();

                for (Map<String, String> map : res) {
                    GroupMember groupMember = setGroupMemberFromIAM(map);
                    members.put(groupMember.getGroupName(), groupMember);
                }

                String key = members.higherKey(marker);
                int num = 0;
                List<GroupMember> memberList = new LinkedList<>();

                while (key != null && num < max) {
                    GroupMember member = members.get(key);
                    memberList.add(member);
                    num++;
                    key = members.higherKey(key);
                }

                groups.setMembers(memberList);
                result.setGroups(groups).setTruncated(members != null);
                response.setListGroupsResult(result);
                return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List groups failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg getGroup(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String groupName = paramMap.get(IAM_GROUP_NAME);
        MsAclUtils.checkIfAnonymous(accountId);

        //用户组名称校验
        if (StringUtils.isEmpty(groupName) || !GROUP_NAME_PATTERN.matcher(groupName).matches()) {
            throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
        }

        GetGroupResponse response = new GetGroupResponse()
                .setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildGetGroupMsg(accountId, groupName);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                response.setGetGroupResult(setGroupResultFromIAM(res));
                msg = buildListUsersForGroupMsg(accountId, groupName);
                ListMapResMsg listMapResMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);

                code = resMsg.getCode();

                if (code == ErrorNo.SUCCESS_STATUS) {
                    GetGroupResponse.Users users = new GetGroupResponse.Users();
                    List<GetGroupResponse.Member> members = new LinkedList<>();
                    for (Map<String, String> re : listMapResMsg.getData()) {
                        String createTime = res.get("createTime"); 
                        String mrn = StringUtils.isNotEmpty(re.get("mrn")) ? re.get("mrn") : re.get("userMRN");
                        GetGroupResponse.Member member = new GetGroupResponse.Member()
                                .setArn(mrn)
                                .setPath("/")
                                .setUserId(re.get("userId"))
                                .setCreateDate(MsDateUtils.stampToDateFormat(createTime))
                                .setUserName(re.get("userName"));

                        members.add(member);
                    }
                    users.setMember(members);
                    response.getGetGroupResult().setUsers(users).setTruncated(false);
                    return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
                } else if (code == ErrorNo.GROUP_NOT_EXISTS) {
                    throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
                }
            } else if (code == ErrorNo.GROUP_NOT_EXISTS) {
                throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get group failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg addUserToGroup(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.get(IAM_USER_NAME);
        String groupName = paramMap.get(IAM_GROUP_NAME);
        MsAclUtils.checkIfAnonymous(accountId);

        if (StringUtils.isEmpty(userName) || !USER_NAME_PATTERN.matcher(userName).matches()) {
            throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
        }

        if (StringUtils.isEmpty(groupName) || !GROUP_NAME_PATTERN.matcher(groupName).matches()) {
            throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
        }

        AddUserToGroupResponse response = new AddUserToGroupResponse()
                .setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildAddUsersToGroupsMsg(accountId, userName, groupName);
            addSyncFlagParam(msg, paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                return new ResponseMsg().setData(response);
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

    public ResponseMsg removeUserFromGroup(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.get(IAM_USER_NAME);
        String groupName = paramMap.get(IAM_GROUP_NAME);
        MsAclUtils.checkIfAnonymous(accountId);
        if (StringUtils.isEmpty(userName) || !USER_NAME_PATTERN.matcher(userName).matches()) {
            throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
        }
        if (StringUtils.isEmpty(groupName) || !GROUP_NAME_PATTERN.matcher(groupName).matches()) {
            throw new MsException(ErrorNo.GROUP_NOT_EXISTS, GROUP_NOT_EXIST_ERR);
        }

        RemoveUsersFromGroupResponse response = new RemoveUsersFromGroupResponse()
                .setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));
        for (int tryTime = 10; ; tryTime -= 1) {
            SocketReqMsg msg = buildRemoveUserFromGroupMsg(accountId, userName, groupName);
            addSyncFlagParam(msg, paramMap);
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                return new ResponseMsg().setData(response);
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

    public ResponseMsg listGroupsForUser(UnifiedMap<String, String> paramMap) {
        //获取url参数值
        String accountId = paramMap.get(USER_ID);
        String userName = paramMap.get(IAM_USER_NAME);
        MsAclUtils.checkIfAnonymous(accountId);
        if (StringUtils.isEmpty(userName) || !USER_NAME_PATTERN.matcher(userName).matches()) {
            throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
        }

        ListGroupsForUserResponse response = new ListGroupsForUserResponse()
                .setResponseMetadata(new ResponseMetadata().setRequestId(paramMap.get(REQUESTID)));
        ListGroupsForUserResponse.ListGroupsForUserResult result = new ListGroupsForUserResponse.ListGroupsForUserResult();
        ListGroupsForUserResponse.Groups groups = new ListGroupsForUserResponse.Groups();
        List<GroupMember> members = new ArrayList<>();
        SocketReqMsg msg = buildListGroupsForUserMsg(accountId, userName);
        for (int tryTime = 10; ; tryTime -= 1) {
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                for (Map<String, String> re : res) {
                    members.add(setGroupMemberFromIAM(re));
                }
                groups.setMember(members);
                result.setGroups(groups);
                result.setTruncated(false);
                response.setListGroupsForUserResult(result);
                return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.USER_NOT_EXISTS) {
                throw new MsException(ErrorNo.USER_NOT_EXISTS, USER_NOT_EXIST_ERR);
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List groups for user failed.");
            }
            sleep(10000);
        }
    }


    private Group setGroupInfoFromIAM(Map<String, String> res) {
        Group group = new Group();
        group.setGroupName(res.get("groupName"));
        group.setGroupId(res.get("groupId"));
        group.setPath("/");
        String createTime = res.get("createTime");
        group.setCreateDate(MsDateUtils.stampToDateFormat(createTime));
        String mrn = StringUtils.isNotEmpty(res.get("mrn")) ? res.get("mrn") : res.get("userMRN");
        group.setArn(mrn);
        return group;
    }

    private GroupMember setGroupMemberFromIAM(Map<String, String> res) {
        GroupMember groupMember = new GroupMember();
        String createTime = res.get("createTime");
        groupMember.setCreateDate(MsDateUtils.stampToDateFormat(createTime));
        groupMember.setGroupName(res.get("groupName"));
        groupMember.setGroupId(res.get("groupId"));
        groupMember.setPath("");
        String mrn = StringUtils.isNotEmpty(res.get("mrn")) ? res.get("mrn") : res.get("userMRN");
        groupMember.setArn(mrn);

        return groupMember;
    }

    private GetGroupResponse.GetGroupResult setGroupResultFromIAM(Map<String, String> res) {
        GetGroupResponse.GetGroupResult result = new GetGroupResponse.GetGroupResult();
        Group group = new Group();
        String createTime = res.get("createTime");
        String mrn = StringUtils.isNotEmpty(res.get("mrn")) ? res.get("mrn") : res.get("userMRN");
        group.setArn(mrn);
        group.setCreateDate(MsDateUtils.stampToDateFormat(createTime));
        group.setGroupName(res.get("groupName"));
        group.setPath("/");
        group.setGroupId(res.get("groupId"));
        result.setGroup(group);

        return result;
    }

    private GroupMember setGroupFromIam(Map<String, String> res) {
        GroupMember member = new GroupMember();
        member.setPath("");
        member.setGroupName(res.get("groupName"));
        String mrn = StringUtils.isNotEmpty(res.get("mrn")) ? res.get("mrn") : res.get("userMRN");
        member.setArn(mrn);
        member.setGroupId(res.get("groupId"));

        return member;
    }
}
