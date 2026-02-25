package com.macrosan.utils.authorize;

import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.Credential;
import com.macrosan.utils.iam.IamUtils;
import com.macrosan.utils.iam.IamUtils.ActionType;
import io.vertx.core.json.Json;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.macrosan.utils.iam.IamUtils.checkTempUserAuth;
import static com.macrosan.utils.iam.IamUtils.checkTempUserAuthFromRocksDb;
import static com.macrosan.utils.sts.RoleUtils.ASSUME_PREFIX;

@Log4j2
public class IamAuth {
    private static final String AUTH_PATH = "/moss/iam/sts_iam_auth.json";
    public static Map<String, Auth[]> authMap = new HashMap<>();

    //支持用户 不输入UserName参数，以当前用户的userName进行调用的action
    public static Set<String> curUserAction = new HashSet<String>() {
        {
            add("ListAccessKeys");
            add("CreateAccessKey");
            add("DeleteAccessKey");
            add("GetUser");
            add("ChangePassword");
        }
    };

    public static Mono<String> auth(String action, String requestUserId,
                                    String accountId, String userName, String groupName, String policyName, String roleName, MsHttpRequest request) {
        Auth[] auths = authMap.get(action);
        return Flux.fromArray(auths)
                .flatMap(auth -> {
                    String resource = auth.resource.replace("accountId", accountId)
                            .replace("groupName", groupName)
                            .replace("policyName", policyName)
                            .replace("userName", userName)
                            .replace("roleName", roleName);
                    if (requestUserId != null && requestUserId.startsWith(ASSUME_PREFIX)) {
                        request.addMember("isRole","true");
                        return checkTempUserAuth(request, requestUserId, ActionType.valueOf(auth.actionType), resource);
                    }
                    return IamUtils.checkAuth(requestUserId, ActionType.valueOf(auth.actionType), resource);
                })
                .collectList()
                .map(l -> l.get(0));
    }

    public static Mono<String> authFromRocksDb(String action, String requestUserId, Credential credential,
                                               String accountId, String userName, String groupName, String policyName, String roleName, MsHttpRequest request) {
        Auth[] auths = authMap.get(action);
        return Flux.fromArray(auths)
                .flatMap(auth -> {
                    String resource = auth.resource.replace("accountId", accountId)
                            .replace("groupName", groupName)
                            .replace("policyName", policyName)
                            .replace("userName", userName)
                            .replace("roleName", roleName);
                    if (requestUserId != null && requestUserId.startsWith(ASSUME_PREFIX)) {
                        request.addMember("isRole","true");
                        return checkTempUserAuthFromRocksDb(request, credential, ActionType.valueOf(auth.actionType), resource);
//                        return checkTempUserAuth(request, requestUserId, ActionType.valueOf(auth.actionType), resource);
                    }
                    return IamUtils.checkAuth(requestUserId, ActionType.valueOf(auth.actionType), resource);
                })
                .collectList()
                .map(l -> l.get(0));
    }

    public static void init() {
        try (FileInputStream stream = new FileInputStream(AUTH_PATH)) {
            byte[] bytes = new byte[1 << 20];
            int n = stream.read(bytes);
            AuthMethods conf = Json.decodeValue(new String(bytes, 0, n), AuthMethods.class);

            for (AuthMethod authMethod : conf.methods) {
                authMap.put(authMethod.name, authMethod.auth);
            }
        } catch (Exception e) {
            log.error("load iam authorization file failed", e);
        }
    }

    @Data
    public static class AuthMethods {
        AuthMethod[] methods;
    }

    @Data
    public static class AuthMethod {
        private Auth[] auth;
        private String name;
    }

    @Data
    public static class Auth {
        private String actionType;
        private String resource;
    }

}
