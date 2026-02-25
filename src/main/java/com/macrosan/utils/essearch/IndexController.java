package com.macrosan.utils.essearch;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.macrosan.action.managestream.EsService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.message.jsonmsg.EsMeta;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.message.jsonmsg.EsMeta.CHECK_SUFFIX;
import static com.macrosan.utils.essearch.EsMetaTask.beforeBulk;
import static com.macrosan.utils.essearch.EsMetaTask.deleteEsMetas;
import static com.macrosan.utils.essearch.EsMetaTaskScanner.RETRY_COUNT;
import static com.macrosan.utils.essearch.EsMetaTaskScanner.scanInfoMap;


/**
 * IndexController
 * ES查询接口
 *
 * @author pangchangya
 * @date 2019/6/3
 */
@Log4j2
public class IndexController {

    private static final Logger delLogger = LogManager.getLogger("DeleteObjLog.IndexController");

    private static RestHighLevelClient client;
    private static IndexController instance;

    private IndexController() {
        client = EsClientFactory.getInstance().getHighLevelClient();
    }

    public static IndexController getInstance() {
        if (instance == null) {
            synchronized (IndexController.class) {
                if (instance == null) {
                    instance = new IndexController();
                }
            }
        }
        return instance;
    }

    public void isLink() throws Exception {
        GetIndexRequest request = new GetIndexRequest();
        request.indices("moss");
        client.indices().exists(request, RequestOptions.DEFAULT);
    }

    /**
     * 向索引下增加数据
     *
     * @param message 上传对象数据
     */
    public void putDataForIndex(String message, String id) throws IOException {

        IndexRequest indexRequest = new IndexRequest("moss", "userid", id)
                .source(message, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        log.debug(indexResponse.getIndex());
    }

    /**
     * 异步批量上传数据
     *
     * @param index      索引
     * @param type       类型
     * @param esMetaList es元数据
     * @param lun        lun
     */
    public void asyncBulkPut(String index, String type, List<EsMeta> esMetaList, List<EsMeta> checkList, String lun, int retry, String currPrefix, Map<String, List<EsMeta>> delMap) {
        try {
            EsMetaTaskScanner.ScanInfo scanInfo = scanInfoMap.get(lun);
            if (retry > RETRY_COUNT) {
                //等待扫描自动重试
                log.error("es bulk retry fail!");
                scanInfo.setRetry(false);
                scanInfo.processor.onNext(new Tuple2<>(scanInfo.getVnode(false), ROCKS_ES_KEY));
                return;
            }

            BulkRequest bulkRequest = new BulkRequest();
            esMetaList.forEach(esMeta -> {
                JSONObject jsonObject = beforeBulk(esMeta);
                if (esMeta.toDelete) {
                    DeleteRequest deleteRequest = (new DeleteRequest(index, type, generateId(jsonObject)));
                    bulkRequest.add(deleteRequest);
                } else if (esMeta.deleteSource) {
                    UpdateRequest updateRequest = new UpdateRequest(index, type, generateId(jsonObject)).doc(ES_DELETE_SOURCE, true);
                    bulkRequest.add(updateRequest);
                } else {
                    IndexRequest indexRequest = (new IndexRequest(index, type, generateId(jsonObject))).source(jsonObject, XContentType.JSON);
                    bulkRequest.add(indexRequest);
                }
            });
            client.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    if (bulkItemResponses.hasFailures()) {
                        scanInfo.setRetry(true);
                        List<EsMeta> successList = new ArrayList<>();
                        List<EsMeta> errorList = new ArrayList<>();
                        log.error("es bulk with failure!");
                        for (int i = 0; i < bulkItemResponses.getItems().length; i++) {
                            BulkItemResponse item = bulkItemResponses.getItems()[i];
                            EsMeta esMeta = esMetaList.get(i);
                            if (item.isFailed()) {
                                errorList.add(esMeta);
                            } else {
                                successList.add(esMeta);
                            }
                        }
                        if (!checkList.isEmpty()) {
                            List<EsMeta> tempList = new ArrayList<>(checkList);
                            successList.removeAll(tempList);
                            tempList.removeAll(errorList);
                            checkList.retainAll(errorList);
                            List<EsMeta> check = delMap.computeIfAbsent(CHECK_SUFFIX, k -> new ArrayList<>());
                            check.addAll(tempList);
                        }
                        deleteEsMetas(successList, delMap, lun, false, false, currPrefix);
                        DISK_SCHEDULER.schedule(() -> asyncBulkPut(index, type, errorList, checkList, lun, retry + 1, currPrefix, delMap), 1, TimeUnit.SECONDS);
                        return;
                    }

                    if (!checkList.isEmpty()) {
                        esMetaList.removeAll(checkList);
                        List<EsMeta> check = delMap.computeIfAbsent(CHECK_SUFFIX, k -> new ArrayList<>());
                        check.addAll(checkList);
                    }
                    deleteEsMetas(esMetaList, delMap, lun, true, false, currPrefix);
                }


                @Override
                public void onFailure(Exception e) {
                    //整个bulk请求全部失败
                    log.error("es bulk all fail ! {}", e.getMessage());
                    scanInfo.setRetry(true);
                    DISK_SCHEDULER.schedule(() -> asyncBulkPut(index, type, esMetaList, checkList, lun, retry + 1, currPrefix, delMap), (long) Math.pow(2, retry), TimeUnit.SECONDS);
                }
            });
        } catch (Exception e) {
            EsMetaTaskScanner.ScanInfo scanInfo = scanInfoMap.get(lun);
            scanInfo.setRetry(false);
            scanInfo.needStop.set(false);
            scanInfo.processor.onNext(new Tuple2<>(scanInfo.getVnode(true), ROCKS_ES_KEY));
            log.error("{}", e.getMessage());
        }
    }

    /**
     * 生成es元数据存储id
     *
     * @param obj json
     */
    public static String generateId(JSONObject obj) {
        return StrToMD5.strMD5(obj.getString("bucketName").concat(obj.getString("path")).concat(obj.getString("versionId")));
    }

    /**
     * 删除数据
     *
     * @param index 索引
     * @param type  类型
     * @param obj   数据信息
     */
    public void delete(String index, String type, JsonObject obj, boolean[] needAsync) throws Exception {
        String id = StrToMD5.strMD5(obj.getString("bucketName").concat(obj.getString("path")).concat(obj.getString("versionId")));
        DeleteRequest request = new DeleteRequest(index, type, id);
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        delLogger.info(response + ", Bucket: {}, Object: {}, versionId: {}", obj.getString("bucketName"), obj.getString("path"), obj.getString("versionId"));
        RestStatus status = response.status();
        if (status == RestStatus.NOT_FOUND) {
            needAsync[0] = true;
        }
    }

    /**
     * ES查询
     *
     * @param map 请求参数
     * @return 返回结果
     */
    public List<String> searchEs(Map<String, String> map, boolean normalSearch) {
        log.debug(map);
        List<String> list = new ArrayList<>();
        JSONArray searchList;
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder qb = QueryBuilders.boolQuery();
        BoolQueryBuilder multiBuilder = QueryBuilders.boolQuery();
        String bucketName = map.get(ES_BUCKET);
        String searchListStr = map.get(ES_SEARCH_LIST);
        String numberStr = map.get(ES_NUMBER);
        String maxNumberStr = map.get(ES_MAX_NUMBER);
        String linkType = map.get(ES_LINK_TYPE);
        SearchRequest searchRequest = new SearchRequest(ES_INDEX);
        if (normalSearch) {
            qb.must(QueryBuilders.termQuery(ES_BUCKET, bucketName));
            try {
                searchList = JSONArray.parseArray(searchListStr);
            } catch (Exception e) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, searchList format error");
            }
            if (searchList.size() == 0) {
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "MetaDateSearch error, searchList size=0");
            }
            // 组合查询
            for (Object object : searchList) {
                Map<String, String> searchMap = (Map<String, String>) object;
                BoolQueryBuilder singleQb = EsService.getInstance().buildEsQuery(searchMap, bucketName);
                if (linkType.equals(ES_TYPE_OR)) {
                    multiBuilder.should(singleQb);
                } else {
                    multiBuilder.must(singleQb);
                }
            }
            qb.must(multiBuilder);
        } else {
            qb.filter(QueryBuilders.termQuery(ES_INODE, map.get(ES_INODE)));
            qb.filter(QueryBuilders.termQuery(ES_BUCKET, bucketName));
            qb.filter(QueryBuilders.termQuery(ES_DELETE_SOURCE, false));
        }
        log.debug("es queryBuilder: " + qb.toString());
        searchSourceBuilder.query(qb);
        int maxNumber = Integer.parseInt(maxNumberStr);
        int number = Integer.parseInt(numberStr);
        searchSourceBuilder.size(maxNumber);
        searchSourceBuilder.from(number);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits searchHits = searchResponse.getHits();
            for (SearchHit hit : searchHits) {
                list.add(hit.getSourceAsString());
            }
        } catch (Exception e) {
            if (e.toString().contains("entity content is too long")) {
                throw new MsException(ErrorNo.ES_SEARCH_BUFFER_EXCEEDED, "MetaDateSearch error, result exceeded es buffer limit");
            }
            log.error("elasticsearch client search error. ", e);
        }
        return list;
    }

    public List<String> serachEsOld(Map<String, String> map) {

        String bucketName = map.get(ES_BUCKET);
        List<String> list = new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest(ES_INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder qb = QueryBuilders.boolQuery();
        qb.must(QueryBuilders.termQuery(ES_BUCKET, bucketName));
        String marker = map.get(ES_MAKER);
        String objName = map.get(ES_OBJ_NAME);
        if (objName == null) {
            objName = "";
        }
        if (marker.equals(ES_ACCURATE)) {
            // 精确查询
            if (!"".equals(objName)) {
                qb.must(QueryBuilders.termQuery("path", objName));
            }
        } else if (marker.equals(ES_VAGUE)) {
            // 模糊查询
            qb.must(QueryBuilders.wildcardQuery("path", "*" + QueryParser.escape(objName) + "*"));
        }
        //时间范围查询
        if (map.containsKey(ES_DATE)) {
            searchDate(qb, map);
        }
        //元数据查询
        if (map.containsKey(ES_META_TYPE)) {
            searchObject(qb, map);
        }
        // 版本号查询
        if (map.containsKey(VERSIONID)) {
            searchVersion(qb, map);
        }

        searchSourceBuilder.query(qb);
        int maxNumber = Integer.parseInt(map.get(ES_MAX_NUMBER));
        int number = Integer.parseInt(map.get(ES_NUMBER));
        searchSourceBuilder.size(maxNumber);
        searchSourceBuilder.from(number);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits searchHits = searchResponse.getHits();
            for (SearchHit hit : searchHits) {
                list.add(hit.getSourceAsString());
            }
        } catch (IOException e) {
            log.error(e);
        }
        return list;
    }

    /**
     * 版本号查询
     *
     * @param qb  查询条件
     * @param map 请求参数
     */
    private static void searchVersion(BoolQueryBuilder qb, Map<String, String> map) {
        String marker = map.get(ES_MAKER);
        String versionId = map.get(VERSIONID);
        if (marker.equals(ES_ACCURATE)) {
            qb.must(QueryBuilders.termQuery("versionId", versionId));
        } else if (marker.equals(ES_VAGUE)) {
            qb.must(QueryBuilders.wildcardQuery("versionId", "*" + QueryParser.escape(versionId) + "*"));
        }
    }

    /**
     * 元数据查询
     *
     * @param qb  查询条件
     * @param map 请求参数
     */

    private static void searchObject(BoolQueryBuilder qb, Map<String, String> map) {
        String marker = map.get(ES_MAKER);
        String useMeta = map.get(ES_USER_META);
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

    /**
     * 分析元数据参数
     *
     * @param qb    查询条件
     * @param key   元数据key值
     * @param value 元数据value值
     */
    private static void analysisValue(BoolQueryBuilder qb, String key, String value) {
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

    private static boolean judgeValue(String value) {
        return value.startsWith("(") && value.endsWith(")") || value.startsWith("(") && value.endsWith("]")
                || value.startsWith("[") && value.endsWith(")") || value.startsWith("[") && value.endsWith("]");
    }

    private static void searchDate(BoolQueryBuilder qb, Map<String, String> map) {
        Long beginDate;
        Long endDate;
        String date = map.get(ES_DATE);
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
        } catch (ParseException e) {
            log.info(e);
            return 0L;
        }
    }
}
