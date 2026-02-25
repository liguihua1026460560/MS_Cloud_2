package com.macrosan.action.managestream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.ec.ErasureClient;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.xmlmsg.MetaSearchResults;
import com.macrosan.message.xmlmsg.OldMetaSearchResults;
import com.macrosan.message.xmlmsg.section.MetaSearchResult;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.essearch.IndexController;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.policy.PolicyCheckUtils;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.text.SimpleDateFormat;
import java.util.*;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;


/**
 * EsService
 * ES查询服务
 *
 * @author pangchangya
 * @date 2019/6/3
 */
public class EsService extends BaseService {

    /**
     * logger日志引用
     **/
    private static Logger logger = LogManager.getLogger(EsService.class.getName());

    private static EsService instance = null;

    private EsService() {
        super();
    }

    /**
     * 每一个Service都必须提供一个getInstance方法
     */
    public static EsService getInstance() {
        if (instance == null) {
            instance = new EsService();
        }
        return instance;
    }

    /**
     * 根据版本号查询元数据
     */
    public ResponseMsg metaDataSearchVersion(UnifiedMap<String, String> paramMap) {
        return metaDataSearch(paramMap);
    }

    /**
     * 根据桶名对象名查询元数据
     */
    public ResponseMsg metaDataSearchObject(UnifiedMap<String, String> paramMap) {
        return metaDataSearch(paramMap);
    }

    private static boolean judgeMeta(String useMeta) {
        try {
            new JsonObject(useMeta);
            return true;
        } catch (Exception e) {
            logger.info(e.getMessage());
            return false;
        }
    }

    // 判断大小参数
    private static void checkSize(String marker, String size) {
        long realSize;
        if (marker.equals(ES_ACCURATE)) {
            try {
                realSize = Long.parseLong(size.trim());
            } catch (Exception e) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "size param format error, must be long");
            }
            if (realSize < 0) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "size param check error, must greater than 0");
            } else if (realSize > 687194767361L){
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "size param check error, must less than 687194767361 Byte");
            }
        } else if (marker.equals(ES_VAGUE)) {
            if (!size.contains(",")) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "size param format error, must contain comma");
            }
            String[] sizeArr = size.split(",");
            if (sizeArr.length > 2 || sizeArr.length == 0) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "size param format error, must only one comma");
            }
            long firstVal;
            long secondVal;
            if (sizeArr.length == 1) {
                String first = sizeArr[0];
                try {
                    firstVal = Long.parseLong(first);
                } catch (Exception e) {
                    throw new MsException(ErrorNo.INVALID_ARGUMENT, "size param format error, must be long");
                }
                if (firstVal < 0) {
                    throw new MsException(ErrorNo.INVALID_ARGUMENT, "size param check error, must greater than 0");
                } else if (firstVal > 687194767361L){
                    throw new MsException(ErrorNo.INVALID_ARGUMENT, "size param check error, must less than 687194767361 Byte");
                }
            } else {
                String first = sizeArr[0];
                String second = sizeArr[1];
                if (StringUtils.isEmpty(first)) {
                    try {
                        secondVal = Long.parseLong(second);
                    } catch (Exception e) {
                        throw new MsException(ErrorNo.INVALID_ARGUMENT, "size param format error, must be long");
                    }
                    if (secondVal < 0) {
                        throw new MsException(ErrorNo.INVALID_ARGUMENT, "size param check error, must greater than 0");
                    } else if (secondVal > 687194767361L){
                        throw new MsException(ErrorNo.INVALID_ARGUMENT, "size param check error, must less than 687194767361 Byte");
                    }
                } else {
                    try {
                        firstVal = Long.parseLong(first);
                        secondVal = Long.parseLong(second);
                    } catch (Exception e) {
                        throw new MsException(ErrorNo.INVALID_ARGUMENT, "size param format error, must be long");
                    }
                    if (firstVal < 0 || secondVal < 0) {
                        throw new MsException(ErrorNo.INVALID_ARGUMENT, "size param check error, must greater than 0");
                    } else if (firstVal > 687194767361L || secondVal > 687194767361L){
                        throw new MsException(ErrorNo.INVALID_ARGUMENT, "size param check error, must less than 687194767361 Byte");
                    }
                    if (secondVal < firstVal) {
                        throw new MsException(ErrorNo.INVALID_ARGUMENT, "size param check error, range format error");
                    }
                }
            }
        }
    }

    private List<MetaSearchResult> analysis(List<String> list) {
        List<MetaSearchResult> esList = new ArrayList<>();
        try {
            for (String source : list) {
                List<String> metaList = new ArrayList<>();
                JsonObject obj = new JsonObject(source);
                for (String str : obj.fieldNames()) {
                    if (str.contains(USER_META)) {
                        metaList.add('"' + str + '"' + ':' + '"' + obj.getValue(str).toString() + '"');
                    }
                }
                String objectName = obj.getString("path");
                MetaSearchResult es = new MetaSearchResult().setMetalists(metaList)
                        .setBucketName(obj.getString("bucketName"))
                        .setLastModified(obj.getString("last-modify"))
                        .setKey(objectName)
                        .setSize(String.valueOf(obj.getValue("size")))
                        .setUserId(obj.getString("userId"))
                        .setETag(obj.getString("eTag"))
                        .setVersionId(obj.getString("versionId"))
                        .setContentType(obj.getString("content-Type"))
                        .setDeleteSource(obj.getBoolean(ES_DELETE_SOURCE) != null && obj.getBoolean(ES_DELETE_SOURCE));
                esList.add(es);
            }
        } catch (Exception e) {
            logger.error("es analysis error. ", e);
        }
        return esList;
    }

    /**
     * 判断传入的lastModified是否符合格式要求
     *
     * @param date 日期参数
     * @return true/false
     */
    private static boolean judgeDate(String date) {
        boolean tag = false;
        if (date.startsWith("(") && date.endsWith(")")) {
            date = date.replace("(", "").replace(")", "");
            tag = isTag(date);
        } else if (date.startsWith("[") && date.endsWith(")")) {
            date = date.replace("[", "").replace(")", "");
            tag = isTag(date);
        } else if (date.startsWith("(") && date.endsWith("]")) {
            date = date.replace("(", "").replace("]", "");
            tag = isTag(date);
        } else if (date.startsWith("[") && date.endsWith("]")) {
            date = date.replace("[", "").replace("]", "");
            tag = isTag(date);
        }
        return tag;
    }

    private static boolean isTag(String date) {
        boolean tag;
        String[] dates = date.split(",");
        boolean between = false;
        try {
            if (date.startsWith(",")) {
                tag = MsDateUtils.judgeDate(dates[1]);
            } else if (date.endsWith(",")) {
                tag = MsDateUtils.judgeDate(dates[0]);
            } else {
                tag = MsDateUtils.judgeDate(dates[0]) && MsDateUtils.judgeDate(dates[1]);
                between = true;
            }
        } catch (Exception e) {
            logger.info(e);
            tag = false;
        }
        if (between && tag && stringToDate(dates[0]) > stringToDate(dates[1])) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, The start time cannot be greater than the end time ");
        }
        return tag;
    }

    /**
     * 判断是否是删除标记的versionid
     */
    private static void checkIsDeleteMarker(String bucketName, String objName, String versionId) {
        StoragePool pool = StoragePoolFactory.getMetaStoragePool(bucketName);
        pool.mapToNodeInfo(pool.getBucketVnodeId(bucketName, objName))
                .flatMap(list -> ErasureClient.getObjectMetaVersionUnlimited(bucketName, objName, versionId, list, null))
                .doOnNext(metaData -> {
                    if (metaData.deleteMarker) {
                        throw new MsException(ErrorNo.METHOD_NOT_ALLOWED, "not allowed to operate deletemarker");
                    }
                })
                .block();
    }

    /**
     * 适配多条件组合查询
     */
    public ResponseMsg metaDataSearch(UnifiedMap<String, String> paramMap) {
        logger.debug(paramMap);
        if(paramMap.get(ES_SEARCH_LIST) != null){
            List<String> list;
            List<MetaSearchResult> esList = new ArrayList<>();
            Map<String, String> map = new HashMap<>();
            int count;
            int nextNumber;
            int number = 0;
            int maxNumber;
            String bucketName = paramMap.get(BUCKET_NAME);
            String linkType = paramMap.get(ES_LINK_TYPE);
            String searchListStr = paramMap.get(ES_SEARCH_LIST);
            String method = "MetaDataSearch";
            if(paramMap.get(VERSIONID) != null){
                method = "MetaDataSearchVersion";
            }
            int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, paramMap.get(USER_ID), method);
            if(policyResult == 0){
                baseCheck(paramMap.get(USER_ID), bucketName, MsAclUtils::checkReadAcl);
            }
            // 检查ES启用状态
            Map<String, String> bucketInfo = getBucketMapByName(bucketName);
            DoubleActiveUtil.siteConstraintCheck(bucketName, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
            String value = Optional.ofNullable(bucketInfo.get("mda")).orElse("off");
            if (!value.equals(ES_ON)) {
                throw new MsException(ErrorNo.BUCKET_SWITCH_OFF, "MetaDateSearch error, bucket metadata switch not opened");
            }
            // 检查linkType、number等参数
            if (linkType == null || (!linkType.equals(ES_TYPE_NORMAL) && !linkType.equals(ES_TYPE_OR) && !linkType.equals(ES_TYPE_AND))) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, linkType not be null");
            }
            if (searchListStr == null) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, searchList not be null");
            }
            if (paramMap.get(ES_MAX_NUMBER) == null) {
                maxNumber = 100;
            } else {
                try {
                    maxNumber = Integer.parseInt(paramMap.get(ES_MAX_NUMBER).trim());
                } catch (Exception e) {
                    throw new MsException(ErrorNo.INVALID_ARGUMENT, "maxNumber param int to string failed");
                }
            }
            Integer maxSearchLimit = Integer.parseInt(Optional.ofNullable(pool.getCommand(REDIS_SYSINFO_INDEX).get("maxSearchLimit")).orElse("100000"));
            if (maxNumber < 1 || maxNumber > maxSearchLimit) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, maxNumber param error, must be int in [1, 100000]");
            }
            if (maxNumber > 200000000) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, maxNumber + number must be less than or equal 200000000");
            }
            map.put(ES_BUCKET, bucketName);
            map.put(ES_MAX_NUMBER, maxNumber + "");
            map.put(ES_NUMBER, number + "");
            map.put(ES_SEARCH_LIST, searchListStr);
            map.put(ES_LINK_TYPE, linkType);

            list = IndexController.getInstance().searchEs(map ,true);
            esList = analysis(list);
            count = esList.size();
            //nextNumber = number + count;
            MetaSearchResults resobj = new MetaSearchResults()
                    .setCount(count)
                    .setResData(esList);

            return new ResponseMsg()
                    .setData(resobj)
                    .addHeader(CONTENT_TYPE, "application/xml");
        }else {
            List<String> list;
            List<MetaSearchResult> esList = new ArrayList<>();
            Map<String, String> map;

            int count;
            int nextNumber;
            int number;
            String bucketName = paramMap.get(BUCKET_NAME);
            String marker = paramMap.get(ES_MAKER);
            String method = "MetaDataSearch";
            if(paramMap.get(VERSIONID) != null){
                method = "MetaDataSearchVersion";
            }

            Map<String, String> bucketInfo = getBucketMapByName(bucketName);
            int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, paramMap.get(USER_ID), method);

            if(policyResult == 0){
                baseCheck(paramMap.get(USER_ID), bucketName, MsAclUtils::checkReadAcl);
            }

            try {
                number = Integer.parseInt(paramMap.get(ES_NUMBER).trim());
            } catch (Exception e) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "number param int to string failed");
            }
            map = checkParams(paramMap, bucketName, marker, number);

            if (number < 0) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "number param int to string failed");
            }
            map.put(ES_NUMBER, number + "");
            try {
                list = IndexController.getInstance().serachEsOld(map);
                esList = analysis(list);
            } catch (Exception e) {
                logger.error(e);
            }
            count = esList.size();
            nextNumber = number + count;
            OldMetaSearchResults resobj = null;
            if (count == 0) {
                resobj = new OldMetaSearchResults()
                        .setCount(count);
            } else if (count < Integer.parseInt(map.get(ES_MAX_NUMBER).trim())) {
                resobj = new OldMetaSearchResults()
                        .setCount(count)
                        .setResData(esList);
            } else {
                try {
                    map.put(ES_NUMBER, nextNumber + "");
                    List<String> list1 = IndexController.getInstance().serachEsOld(map);
                    if (list1.size() > 0) {
                        resobj = new OldMetaSearchResults()
                                .setCount(count)
                                .setNextNumber(nextNumber)
                                .setResData(esList);
                    } else {
                        resobj = new OldMetaSearchResults()
                                .setCount(count)
                                .setResData(esList);
                    }
                } catch (Exception e) {
                    logger.error(e);
                }
            }

            return new ResponseMsg()
                    .setData(resobj)
                    .addHeader(CONTENT_TYPE, "application/xml");
        }


    }

    /**
     * 校验请求参数
     *
     * @param paramMap   请求参数
     * @param bucketName 同名称
     * @param marker     查询方式
     * @return 请求参数
     */
    private Map<String, String> checkParams(UnifiedMap<String, String> paramMap, String bucketName, String marker, int number) {
        Map<String, String> map = new HashMap<>();
        int maxNumber;
        String metaType = null;
        String useMeta = paramMap.get(ES_USER_META);
        String objName = paramMap.get(ES_OBJECT_NAME);
        String lastModified = paramMap.get(ES_LAST_MODIFIED);
        String contentType = paramMap.get(ES_CONTENT_TYPE);
        String eTag = paramMap.get(ES_ETAG);
        String accountId = paramMap.get(ES_ACCOUNT_ID);
        String size = paramMap.get(ES_SIZE);
        Map<String, String> bucketInfo = getBucketMapByName(bucketName);
        String value = Optional.ofNullable(bucketInfo.get("mda")).orElse("off");
        if (!value.equals(ES_ON)) {
            throw new MsException(ErrorNo.BUCKET_SWITCH_OFF, "MetaDateSearch error, bucket metadata switch not opened");
        }
        if (useMeta == null && objName == null && lastModified == null &&
                contentType == null && eTag == null && accountId == null && size == null) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, userMeta、objName、lastModified、contentType、eTag、accountId and objSize at least one is not empty");
        }
        if (useMeta != null) {
            if (useMeta.contains("\\")) {
                useMeta = useMeta.replace("\\", "\\\\");
            }
            if (!useMeta.contains(USER_META)) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, incorrect metadata format");
            } else if (!judgeMeta(useMeta)) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, incorrect metadata format");
            } else {
                metaType = ES_ON;
                map.put(ES_META_TYPE, metaType);
            }
        }
        if (paramMap.containsKey(ES_LAST_MODIFIED)) {
            if (judgeDate(paramMap.get(ES_LAST_MODIFIED))) {
                map.put(ES_DATE, paramMap.get(ES_LAST_MODIFIED));
            } else {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, date format error");
            }
        }
        if (paramMap.get(ES_MAKER) == null) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, marker not be null");
        }
        if (!marker.equals(ES_ACCURATE) && !marker.equals(ES_VAGUE)) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, marker must be vague or accurate");
        }
        if (paramMap.get(ES_NUMBER) == null) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, number not be null");
        }
        if (paramMap.get(ES_MAX_NUMBER) == null) {
            maxNumber = 100;
        } else {
            try {
                maxNumber = Integer.parseInt(paramMap.get(ES_MAX_NUMBER).trim());
            } catch (Exception e) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "maxNumber param int to string failed");
            }
        }
        if (maxNumber < 1 || maxNumber > 1000) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, maxNumber param error, must be int in [1, 1000]");
        }
        if (number + maxNumber > 200000000) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, maxNumber + number must be less than or equal 200000000");
        }
        // 版本id
        if (paramMap.get(VERSIONID) != null) {
            // 是否开启多版本
            if ("Enabled".equals(bucketInfo.get(BUCKET_VERSION_STATUS))) {
                map.put(VERSIONID, paramMap.get(VERSIONID));
                checkIsDeleteMarker(bucketName, objName, map.get(VERSIONID));
            }
        }
        if (!StringUtils.isEmpty(paramMap.get(ES_CONTENT_TYPE))) {
            map.put(ES_CONTENT_TYPE, paramMap.get(ES_CONTENT_TYPE));
        }
        if (!StringUtils.isEmpty(paramMap.get(ES_ACCOUNT_ID))) {
            map.put(ES_ACCOUNT_ID, paramMap.get(ES_ACCOUNT_ID));
        }
        if (!StringUtils.isEmpty(paramMap.get(ES_ETAG))) {
            map.put(ES_ETAG, paramMap.get(ES_ETAG));
        }
        if (!StringUtils.isEmpty(paramMap.get(ES_SIZE))) {
            //判断格式
            checkSize(marker, size);
            map.put(ES_SIZE, paramMap.get(ES_SIZE));
        }
        map.put(ES_BUCKET, bucketName);
        map.put(ES_MAKER, marker);
        map.put(ES_MAX_NUMBER, maxNumber + "");
        map.put(ES_OBJ_NAME, objName);
        map.put(ES_USER_META, useMeta);
        return map;
    }

    // 构建请求体
    public BoolQueryBuilder buildEsQuery(Map<String, String> searchMap, String bucketName) {
        BoolQueryBuilder qb = QueryBuilders.boolQuery();
        String marker = searchMap.get(ES_MAKER);
        String useMeta = searchMap.get(ES_USER_META);
        String objName = searchMap.get(ES_OBJ_NAME);
        String lastModified = searchMap.get(ES_LAST_MODIFIED);
        String versionId = searchMap.get(VERSIONID);
        String contentType = searchMap.get(ES_CONTENT_TYPE);
        String eTag = searchMap.get(ES_ETAG);
        String accountId = searchMap.get(ES_ACCOUNT_ID);
        String size = searchMap.get(ES_SIZE);
        // 检验参数
        if (marker == null) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, marker not be null");
        }
        if (!marker.equals(ES_ACCURATE) && !marker.equals(ES_VAGUE) && !marker.equals(ES_PREFIX) && !marker.equals(ES_SUFFIX)) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, marker must be vague、accurate、prefix or suffix");
        }
        if ((marker.equals(ES_PREFIX) || marker.equals(ES_SUFFIX)) && !searchMap.containsKey(ES_OBJ_NAME)) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, only objName serach can use prefix or suffix marker");
        }
        if (useMeta == null && objName == null && lastModified == null &&
                contentType == null && eTag == null && accountId == null && size == null && versionId == null) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, userMeta、objName、lastModified、contentType、eTag、accountId and objSize at least one is not empty");
        }
        if (ES_ACCURATE.equals(marker)) {
            if (useMeta != null && useMeta.isEmpty() || objName != null && objName.isEmpty()
                    || lastModified != null && lastModified.isEmpty() || contentType != null && contentType.isEmpty() || eTag != null && eTag.isEmpty()
                    || accountId != null && accountId.isEmpty() || size != null && size.isEmpty() || versionId != null && versionId.isEmpty()) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error,  accurate search condition can not be empty");
            }
        }
        // 构建对象名查询
        if (objName != null && versionId == null) {
            searchObjName(qb, marker, objName);
        } else if (versionId != null && objName != null) {
            // 是否开启多版本
            Map<String, String> bucketInfo = getBucketMapByName(bucketName);
            if ("Enabled".equals(bucketInfo.get(BUCKET_VERSION_STATUS))) {
                checkIsDeleteMarker(bucketName, objName, versionId);
            }
            searchVersion(qb, marker, versionId, objName);
        }
        // 构建用户元数据查询
        if (useMeta != null) {
//            if (useMeta.contains("\\")) {
//                useMeta = useMeta.replace("\\", "\\\\");
//            }
            if (!useMeta.toLowerCase().contains(USER_META)) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, incorrect metadata format");
            } else if (!judgeMeta(useMeta)) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, incorrect metadata format");
            }
            searchObject(qb, marker, useMeta);
        }
        // 构建日期查询
        if (lastModified != null) {
            if (judgeDate(lastModified)) {
                searchDate(qb, lastModified);
            } else {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, date format error");
            }
        }
        // 构建类型查询
        if (contentType != null) {
            searchOthers(qb, "content-Type", marker, contentType);
        }
        // 构建账户ID查询
        if (accountId != null) {
            searchOthers(qb, "userId", marker, accountId);
        }
        // 构建ETag查询
        if (eTag != null) {
            searchOthers(qb, "eTag", marker, eTag.toLowerCase());
        }
        // 构建对象大小查询
        if (!StringUtils.isEmpty(size)) {
            //判断格式
            checkSize(marker, size);
            searchSize(qb, marker, size);
        }
        return qb;
    }

    /**
     * 元数据查询
     */

    private void searchObject(BoolQueryBuilder qb, String marker, String useMeta) {
        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        String key;
        String value;
        JsonObject obj = new JsonObject(useMeta);
        for (String str : obj.fieldNames()) {
            key = str;
            value = obj.getString(key);
            keys.add(key);
            values.add(value);
        }
        boolean tag = false;
        if (marker.equals(ES_ACCURATE)) {
            //精确查询
            for (int i = 0; i < keys.size(); i++) {
                tag = judgeValue(values.get(i));
                if (tag) {
                    analysisValue(qb, keys.get(i), values.get(i));
                } else {
                    qb.must(QueryBuilders.termQuery(keys.get(i), values.get(i)));
                }
            }
        } else if (marker.equals(ES_VAGUE)) {
            //模糊查询
            for (int i = 0; i < keys.size(); i++) {
                tag = judgeValue(values.get(i));
                if (tag) {
                    analysisValue(qb, keys.get(i), values.get(i));
                } else {
                    qb.must(QueryBuilders.wildcardQuery(keys.get(i), "*" + QueryParser.escape(values.get(i)) + "*"));
                }
            }
        }
    }

    private boolean judgeValue(String value) {
        return value.startsWith("(") && value.endsWith(")") || value.startsWith("(") && value.endsWith("]")
                || value.startsWith("[") && value.endsWith(")") || value.startsWith("[") && value.endsWith("]");
    }

    /**
     * 分析元数据参数
     *
     * @param qb    查询条件
     * @param key   元数据key值
     * @param value 元数据value值
     */
    private void analysisValue(BoolQueryBuilder qb, String key, String value) {
        //大于和小于
        if (value.startsWith("(") && value.endsWith(")")) {
            value = value.replace("(", "").replace(")", "");
            String[] values = value.split(",");
            if (value.startsWith(",")) {
                qb.must(QueryBuilders.rangeQuery(key).lt(values[1]));
            } else if (value.endsWith(",")) {
                qb.must(QueryBuilders.rangeQuery(key).gt(values[0]));
            } else {
                qb.must(QueryBuilders.rangeQuery(key).gt(values[0]).lt(values[1]));
            }

        }
        //大于等于和小于
        else if (value.startsWith("[") && value.endsWith(")")) {
            value = value.replace("[", "").replace(")", "");
            String[] values = value.split(",");
            if (value.startsWith(",")) {
                qb.must(QueryBuilders.rangeQuery(key).lt(values[1]));
            } else if (value.endsWith(",")) {
                qb.must(QueryBuilders.rangeQuery(key).gte(values[0]));
            } else {
                qb.must(QueryBuilders.rangeQuery(key).gte(values[0]).lt(values[1]));
            }
        }
        //大于和小于等于
        else if (value.startsWith("(") && value.endsWith("]")) {
            value = value.replace("(", "").replace("]", "");
            String[] values = value.split(",");
            if (value.startsWith(",")) {
                qb.must(QueryBuilders.rangeQuery(key).lte(values[1]));
            } else if (value.endsWith(",")) {
                qb.must(QueryBuilders.rangeQuery(key).gt(values[0]));
            } else {
                qb.must(QueryBuilders.rangeQuery(key).gt(values[0]).lte(values[1]));
            }
        }
        //大于等于和小于等于
        else if (value.startsWith("[") && value.endsWith("]")) {
            value = value.replace("[", "").replace("]", "");
            String[] values = value.split(",");
            if (value.startsWith(",")) {
                qb.must(QueryBuilders.rangeQuery(key).lte(values[1]));
            } else if (value.endsWith(",")) {
                qb.must(QueryBuilders.rangeQuery(key).gte(values[0]));
            } else {
                qb.must(QueryBuilders.rangeQuery(key).gte(values[0]).lte(values[1]));
            }
        }
    }

    /**
     * 分析时间参数
     */
    private void searchDate(BoolQueryBuilder qb, String date) {
        Long beginDate;
        Long endDate;
        //大于和小于
        if (date.startsWith("(") && date.endsWith(")")) {
            date = date.replace("(", "").replace(")", "");
            String[] dates = date.split(",");
            if (date.startsWith(",")) {
                endDate = stringToDate(dates[1]);
                qb.must(QueryBuilders.rangeQuery(ES_DATE).lt(endDate));
            } else if (date.endsWith(",")) {
                beginDate = stringToDate(dates[0]);
                qb.must(QueryBuilders.rangeQuery(ES_DATE).gt(beginDate));
            } else {
                beginDate = stringToDate(dates[0]);
                endDate = stringToDate(dates[1]);
                qb.must(QueryBuilders.rangeQuery(ES_DATE).gt(beginDate).lt(endDate));
            }

        }
        //大于等于和小于
        if (date.startsWith("[") && date.endsWith(")")) {
            date = date.replace("[", "").replace(")", "");
            String[] dates = date.split(",");
            if (date.startsWith(",")) {
                endDate = stringToDate(dates[1]);
                qb.must(QueryBuilders.rangeQuery(ES_DATE).lt(endDate));
            } else if (date.endsWith(",")) {
                beginDate = stringToDate(dates[0]);
                qb.must(QueryBuilders.rangeQuery(ES_DATE).gte(beginDate));
            } else {
                beginDate = stringToDate(dates[0]);
                endDate = stringToDate(dates[1]);
                qb.must(QueryBuilders.rangeQuery(ES_DATE).gte(beginDate).lt(endDate));
            }
        }
        //大于和小于等于
        if (date.startsWith("(") && date.endsWith("]")) {
            date = date.replace("(", "").replace("]", "");
            String[] dates = date.split(",");
            if (date.startsWith(",")) {
                endDate = stringToDate(dates[1]);
                qb.must(QueryBuilders.rangeQuery(ES_DATE).lte(endDate));
            } else if (date.endsWith(",")) {
                beginDate = stringToDate(dates[0]);
                qb.must(QueryBuilders.rangeQuery(ES_DATE).gt(beginDate));
            } else {
                beginDate = stringToDate(dates[0]);
                endDate = stringToDate(dates[1]);
                qb.must(QueryBuilders.rangeQuery(ES_DATE).gt(beginDate).lte(endDate));
            }
        }
        //大于等于和小于等于
        if (date.startsWith("[") && date.endsWith("]")) {
            date = date.replace("[", "").replace("]", "");
            String[] dates = date.split(",");
            if (date.startsWith(",")) {
                endDate = stringToDate(dates[1]);
                qb.must(QueryBuilders.rangeQuery(ES_DATE).lte(endDate));
            } else if (date.endsWith(",")) {
                beginDate = stringToDate(dates[0]);
                qb.must(QueryBuilders.rangeQuery(ES_DATE).gte(beginDate));
            } else {
                beginDate = stringToDate(dates[0]);
                endDate = stringToDate(dates[1]);
                qb.must(QueryBuilders.rangeQuery(ES_DATE).gte(beginDate).lte(endDate));
            }
        }
    }

    /**
     * 字符串转日期
     *
     * @param time 请求参数
     * @return 返回结果
     */
    private static Long stringToDate(String time) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS Z");
            Date date = sdf.parse(time.replace("Z", " UTC"));
            return date.getTime();
        } catch (Exception e) {
            logger.error("stringToDate error. ", e);
            return 0L;
        }
    }

    // 对象名查询
    private void searchObjName(BoolQueryBuilder qb, String marker, String objName) {
        if (objName == null) {
            objName = "";
        }
        switch (marker) {
            case ES_ACCURATE:
                // 精确查询
//                if (!"".equals(objName)) {
                qb.must(QueryBuilders.termQuery("path", objName));
//                }
                break;
            case ES_VAGUE:
                // 模糊查询
                qb.must(QueryBuilders.wildcardQuery("path", "*" + QueryParser.escape(objName) + "*"));
                break;
            case ES_PREFIX:
                // 前缀查询
                qb.must(QueryBuilders.prefixQuery("path", objName));
                break;
            case ES_SUFFIX:
                // 后缀查询
                qb.must(QueryBuilders.wildcardQuery("path", "*" + QueryParser.escape(objName)));
                break;
            default:
                break;
        }
    }

    /**
     * 版本号查询
     */
    private void searchVersion(BoolQueryBuilder qb, String marker, String versionId, String objName) {
        if (marker.equals(ES_ACCURATE)) {
            qb.must(QueryBuilders.termQuery("versionId", versionId)).must(QueryBuilders.termQuery("path", objName));
        } else if (marker.equals(ES_VAGUE)) {
            qb.must(QueryBuilders.wildcardQuery("versionId", "*" + QueryParser.escape(versionId) + "*")).must(QueryBuilders.termQuery("path", objName));
        }
    }

    /**
     * 查询类型 账户 etag
     */
    private void searchOthers(BoolQueryBuilder qb, String filed, String marker, String value) {
        if (marker.equals(ES_ACCURATE)) {
            qb.must(QueryBuilders.termQuery(filed, value));
        } else if (marker.equals(ES_VAGUE)) {
            qb.must(QueryBuilders.wildcardQuery(filed, "*" + QueryParser.escape(value) + "*"));
        }
    }

    /**
     * 查询大小
     */
    private void searchSize(BoolQueryBuilder qb, String marker, String value) {
        if (marker.equals(ES_ACCURATE)) {
            qb.must(QueryBuilders.termQuery("size", Long.valueOf(value)));
        } else if (marker.equals(ES_VAGUE)) {
            // 区分大于和小于
            String[] values = value.split(",");
            if (value.startsWith(",")) {
                qb.must(QueryBuilders.rangeQuery("size").gte(0L).lte(Long.valueOf(values[1])));
            } else if (value.endsWith(",")) {
                qb.must(QueryBuilders.rangeQuery("size").gte(Long.valueOf(values[0])).lte(687194767361L));
            } else {
                qb.must(QueryBuilders.rangeQuery("size").gte(Long.valueOf(values[0])).lte(Long.valueOf(values[1])));
            }
        }
    }
}
