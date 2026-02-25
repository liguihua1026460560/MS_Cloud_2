package com.macrosan.utils.notification;

import com.macrosan.httpserver.MsHttpRequest;
import lombok.extern.log4j.Log4j2;

import static com.macrosan.constants.ServerConstants.VERSIONID;
import static com.macrosan.constants.ServerConstants.X_AMX_DELETE_MARKER;

@Log4j2
public class NotificationFilter {


    /**
     * 判定请求是否为上传，复制对象操作
     *
     * @param request
     * @return
     */

    public static boolean judgePut(MsHttpRequest request, String prefix, String suffix) {
        String method = request.rawMethod();
        String objectName = request.getObjectName();
        String partNumber = request.getParam("partNumber");
        String uploadId = request.getParam("uploadId");

        if (checkNull(method) || checkNull(objectName) || !checkNull(partNumber) || !checkNull(uploadId)) {
            return false;
        }

        if (method == "PUT" && !request.headers().contains("x-amz-copy-source")) {
            return checkPrefixAndSuffix(objectName, prefix, suffix);
        }

        if ("POST".equalsIgnoreCase(method) && request.params().contains("append") && request.params().contains("position")) {
            return checkPrefixAndSuffix(objectName, prefix, suffix);
        }

        return false;
    }

    /**
     * 判断请求类型是否为分段上传
     *
     * @param request
     * @return
     */

    public static boolean judgeCopy(MsHttpRequest request, String prefix, String suffix) {
        String method = request.rawMethod();
        String objectName = request.getObjectName();
        String partNumber = request.getParam("partNumber");
        String uploadId = request.getParam("uploadId");

        if (checkNull(method) || checkNull(objectName) || !checkNull(partNumber) || !checkNull(uploadId)) {
            return false;
        }

        if (method == "PUT" && request.headers().contains("x-amz-copy-source")) {
            return checkPrefixAndSuffix(objectName, prefix, suffix);
        }

        return false;
    }

    /**
     * 判断请求是否为完成分段上传任务的请求
     *
     * @param request
     * @return
     */
    public static boolean judgeMultiComplete(MsHttpRequest request, String prefix, String suffix) {
        String method = request.rawMethod();
        String objectName = request.getObjectName();
        String uploadId = request.getParam("uploadId");

        if (checkNull(method) || checkNull(objectName) || checkNull(uploadId)) {
            return false;
        }

        if (method == "POST") {
            return checkPrefixAndSuffix(objectName, prefix, suffix);
        }

        return false;
    }

    /**
     * 判断是否为删除
     *
     * @param request
     * @return
     */
    public static boolean judgeDelete(MsHttpRequest request, String prefix, String suffix) {
        String method = request.rawMethod();
        String objectName = request.getObjectName();

        if (checkNull(method) || checkNull(objectName)) {
            return false;
        }

        //不开启多版本
        boolean mark = request.response().headers().contains(X_AMX_DELETE_MARKER);

        if (!mark && method.equals("DELETE")) {
            return checkPrefixAndSuffix(objectName, prefix, suffix);
        }

        //开启多版本
        boolean requestVersion = request.params().contains(VERSIONID);
        if (requestVersion && method.equals("DELETE")) {
            return checkPrefixAndSuffix(objectName, prefix, suffix);
        }


        return false;
    }

    public static boolean judgeDeleteMarkerCreated(MsHttpRequest request, String prefix, String suffix) {
        String method = request.rawMethod();
        String objectName = request.getObjectName();


        if (checkNull(method) || checkNull(objectName)) {
            return false;
        }

        boolean requestVersion = request.params().contains(VERSIONID);
        boolean mark = request.response().headers().contains(X_AMX_DELETE_MARKER);
        if (!requestVersion && method.equals("DELETE") && mark) {
            return checkPrefixAndSuffix(objectName, prefix, suffix);
        }

        return false;
    }

    public static boolean judgeMultiDelete(MsHttpRequest request, String objectName, String prefix, String suffix) {
        String method = request.rawMethod();


        if (checkNull(method) || !request.params().contains("delete")) {
            return false;
        }

        if (method == "POST") {
            return checkPrefixAndSuffix(objectName, prefix, suffix);
        }


        return false;
    }

    public static boolean judgeMultiDelete(MsHttpRequest request) {
        String method = request.rawMethod();


        if (checkNull(method) || !request.params().contains("delete")) {
            return false;
        }

        if (method == "POST") {
            return true;
        }


        return false;
    }

    public static boolean checkNull(String str) {
        if (str == null || str.isEmpty()) {
            return true;
        }
        return false;
    }

    public static boolean checkPrefixAndSuffix(String objectName, String prefix, String suffix) {
        //当只有suffix有值
        if (checkNull(prefix) && !checkNull(suffix)) {
            return objectName.endsWith(suffix);
        }
        //当只有prefix有值
        if (!checkNull(prefix) && checkNull(suffix)) {
            return objectName.startsWith(prefix);
        }
        //当prefix和suffix全为空时
        if (checkNull(prefix) && checkNull(suffix)) {
            return true;
        }
        //当prefix和suffix全部不为空时
        if (objectName.startsWith(prefix)) {
            if (objectName.endsWith(suffix)) {
                return true;
            }
            return false;
        }
        return false;
    }
}
