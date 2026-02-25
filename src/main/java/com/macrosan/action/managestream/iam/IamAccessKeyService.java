package com.macrosan.action.managestream.iam;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.socketmsg.ListMapResMsg;
import com.macrosan.message.socketmsg.MapResMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.iam.CreateAccessKeyResponse;
import com.macrosan.message.xmlmsg.iam.CreateAccessKeyResponse.AccessKey;
import com.macrosan.message.xmlmsg.iam.CreateAccessKeyResponse.CreateAccessKeyResult;
import com.macrosan.message.xmlmsg.iam.DeleteAccessKeyResponse;
import com.macrosan.message.xmlmsg.iam.ListAccessKeysResponse;
import com.macrosan.message.xmlmsg.iam.ListAccessKeysResponse.AccessKeyMetadata;
import com.macrosan.message.xmlmsg.iam.ListAccessKeysResponse.ListAccessKeysResult;
import com.macrosan.message.xmlmsg.iam.ListAccessKeysResponse.Member;
import com.macrosan.message.xmlmsg.iam.ResponseMetadata;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.macrosan.constants.IAMConstants.AWS_IAM_USER_NAME;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.message.consturct.SocketReqMsgBuilder.*;
import static com.macrosan.utils.functional.exception.Sleep.sleep;
import static com.macrosan.utils.regex.PatternConst.ACCESSKEY_PATTERN;
import static com.macrosan.utils.regex.PatternConst.SECRETKEY_PATTERN;

@Log4j2
public class IamAccessKeyService extends BaseService {
    private static IamAccessKeyService instance = null;

    private IamAccessKeyService() {
        super();
    }

    /**
     * 每一个Service都必须提供一个getInstance方法
     */
    public static IamAccessKeyService getInstance() {
        if (instance == null) {
            instance = new IamAccessKeyService();
        }
        return instance;
    }

    public ResponseMsg listAccessKeys(UnifiedMap<String, String> paramMap) {
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);

        String userName = paramMap.get(AWS_IAM_USER_NAME);
        if (userName == null) {
            userName = paramMap.get(USERNAME);
        }

        SocketReqMsg msg;

        if (StringUtils.isNotBlank(userName)) {
            msg = buildListAccessKeysMsg(accountId, userName);
        } else {
            msg = buildListAccountAccessKeysMsg(accountId, "");
        }


        List<Member> akList = new ArrayList<>(4);
        AccessKeyMetadata accessKeyMetadata = new AccessKeyMetadata()
                .setMember(akList);

        ListAccessKeysResult result = new ListAccessKeysResult()
                .setAccessKeyMetadata(accessKeyMetadata)
                .setTruncated(false);

        ListAccessKeysResponse response = new ListAccessKeysResponse()
                .setListAccessKeysResult(result)
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(paramMap.get(REQUESTID)));

        for (int tryTime = 10; ; tryTime -= 1) {
            ListMapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, ListMapResMsg.class, true);
            List<Map<String, String>> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                for (Map<String, String> re : res) {
                    akList.add(getAkMemberFromIam(re, userName));
                }

                return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.NO_SUCH_ACCOUNT) {
                throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "The account does not exist.");
            } else if(code == ErrorNo.USER_NOT_EXISTS){
                throw new MsException(ErrorNo.USER_NOT_EXISTS, "The user not exist. ");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "List account ak failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg createAccessKey(UnifiedMap<String, String> paramMap) {
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);

        String userName = paramMap.get(AWS_IAM_USER_NAME);
        if (userName == null) {
            userName = paramMap.get(USERNAME);
        }

        SocketReqMsg msg;
        if (StringUtils.isBlank(userName)) {
            msg = new SocketReqMsg(MSG_TYPE_CREATE_ACCESSKEY_ACCOUNT, 0);
            msg.put("accountId", accountId);
        } else {
            msg = new SocketReqMsg(MSG_TYPE_CREATE_ACCESSKEY_USER, 0);
            msg.put("accountId", accountId);
            msg.put("userName", userName);
        }

        String ak = paramMap.get("moss_ak");
        String sk = paramMap.get("moss_sk");

        if (!StringUtils.isBlank(ak) && !StringUtils.isBlank(sk)) {
            if (!ACCESSKEY_PATTERN.matcher(ak).matches()) {
                throw new MsException(ErrorNo.ACCESSKEY_NOT_MATCH, "The input ak is invalid.");
            }
            if (!SECRETKEY_PATTERN.matcher(sk).matches()) {
                throw new MsException(ErrorNo.SECRETKEY_NOT_MATCH, "The input sk is invalid.");
            }
            msg.put("ak", ak);
            msg.put("sk", sk);
            msg.put("createTime", System.currentTimeMillis() + "");
            msg.put(SITE_FLAG, "1");
        }

        CreateAccessKeyResult createAccessKeyResult = new CreateAccessKeyResult();

        CreateAccessKeyResponse response = new CreateAccessKeyResponse()
                .setCreateAccessKeyResult(createAccessKeyResult)
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(paramMap.get(REQUESTID)));

        for (int tryTime = 10; ; tryTime -= 1) {
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            Map<String, String> res = resMsg.getData();
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                AccessKey accessKey = getAccessKeyFromIAM(res);
                accessKey.setUserName(userName);
                createAccessKeyResult.setAccessKey(accessKey);
                return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.NO_SUCH_ACCOUNT) {
                throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "The account is not exist.");
            } else if (code == ErrorNo.TOO_MANY_AK) {
                throw new MsException(ErrorNo.TOO_MANY_AK, "The account has over two aks.");
            } else if (code == ErrorNo.AK_EXISTS) {
                throw new MsException(ErrorNo.AK_EXISTS, "The ak exist.");
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            }else if(code == ErrorNo.USER_NOT_EXISTS){
                throw new MsException(ErrorNo.USER_NOT_EXISTS, "The user not exist. ");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Create ak failed.");
            }
            sleep(10000);
        }
    }

    public ResponseMsg deleteAccessKey(UnifiedMap<String, String> paramMap) {
        String accountId = paramMap.get(USER_ID);
        MsAclUtils.checkIfAnonymous(accountId);

        String userName = paramMap.get(AWS_IAM_USER_NAME);
        if (userName == null) {
            userName = paramMap.get(USERNAME);
        }

        String accessKeyId = paramMap.get("AccessKeyId");

        SocketReqMsg msg;

        if (StringUtils.isBlank(userName)) {
            msg = buildDeleteAccessKeyAccountMsg(accountId, accessKeyId);
        } else {
            msg = buildDeleteAccessKeyMsg(accountId, userName, accessKeyId);
        }

        DeleteAccessKeyResponse response = new DeleteAccessKeyResponse()
                .setResponseMetadata(new ResponseMetadata()
                        .setRequestId(paramMap.get(REQUESTID)));

        for (int tryTime = 10; ; tryTime -= 1) {
            MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
            int code = resMsg.getCode();

            if (code == ErrorNo.SUCCESS_STATUS) {
                return new ResponseMsg().setData(response).addHeader(CONTENT_TYPE, XML_RESPONSE);
            } else if (code == ErrorNo.NO_SUCH_ACCOUNT) {
                throw new MsException(ErrorNo.NO_SUCH_ACCOUNT, "The account is not exist.");
            } else if (code == ErrorNo.AK_NOT_EXISTS) {
                throw new MsException(ErrorNo.AK_NOT_EXISTS, "The ak does not exist.");
            } else if (code == ErrorNo.MOSS_FAIL) {
                throw new MsException(ErrorNo.MOSS_FAIL, "MOSS operation failed.");
            } else if(code == ErrorNo.USER_NOT_EXISTS){
                throw new MsException(ErrorNo.USER_NOT_EXISTS, "The user not exist. ");
            }
            if (tryTime == 0) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "Delete ak failed.");
            }
            sleep(10000);
        }
    }


    public static Member getAkMemberFromIam(Map<String, String> res, String userName) {
        String createTime = res.get("createTime");
        String stamp = createTime.contains(":") ? String.valueOf(MsDateUtils.dateToStamp(createTime)) : createTime;
        createTime = MsDateUtils.stampToDateFormat(stamp);


        return new Member()
                .setAccessKeyId(res.get("ak"))
                .setStatus("Active")
                .setUserName(userName)
                .setCreateDate(createTime);
    }

    public static AccessKey getAccessKeyFromIAM(Map<String, String> res) {
        String createTime = res.get("createTime");
        String stamp = createTime.contains(":") ? String.valueOf(MsDateUtils.dateToStamp(createTime)) : createTime;
        createTime = MsDateUtils.stampToDateFormat(stamp);
        return new AccessKey()
                .setAccessKeyId(res.get("ak"))
                .setSecretAccessKey(res.get("sk"))
                .setCreateDate(createTime)
                .setStatus("Active");
    }
}
