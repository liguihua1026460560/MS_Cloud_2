package com.macrosan.action.datastream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.ec.ECUtils;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.consturct.RequestBuilder;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.xmlmsg.HttpMethodTrafficStatisticsResult;
import com.macrosan.message.xmlmsg.ObjectStatisticsResult;
import com.macrosan.message.xmlmsg.TrafficStatisticsResult;
import com.macrosan.message.xmlmsg.TrafficStatisticsResultList;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.storage.client.ListRecodeHandler;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsDateUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.quota.QuotaRecorder;
import com.macrosan.utils.quota.StatisticsRecorder;
import com.macrosan.utils.regex.PatternConst;
import com.macrosan.utils.serialize.JaxbUtils;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

import java.util.*;
import java.util.stream.Collectors;

import static com.macrosan.constants.ErrorNo.NO_SUCH_BUCKET;
import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.GET_TRAFFIC_STATISTICS;
import static com.macrosan.httpserver.ResponseUtils.addAllowHeader;
import static com.macrosan.httpserver.ResponseUtils.addPublicHeaders;
import static com.macrosan.utils.msutils.MsException.throwWhenEmpty;
import static com.macrosan.utils.quota.StatisticsRecorder.STATISTIC_STORAGE_POOL;

/**
 * 获取统计信息
 *
 * @author xiangzicheng-PC
 * @create 2020/6/2
 * @since 1.0.0
 */
@Log4j2
public class StatisticsService extends BaseService {
    private static StatisticsService instance = null;

    public static StatisticsService getInstance() {
        if (instance == null) {
            instance = new StatisticsService();
        }
        return instance;
    }

    public static final Set<String> REQUEST_TYPES = new HashSet<>(Arrays.asList(
            MGT_STAT_REQ_TYPE_READ, MGT_STAT_REQ_TYPE_WRITE, MGT_STAT_REQ_TYPE_TOTAL,
            MGT_STAT_REQ_TYPE_GET, MGT_STAT_REQ_TYPE_DELETE, MGT_STAT_REQ_TYPE_PUT, MGT_STAT_REQ_TYPE_POST, MGT_STAT_REQ_TYPE_HEAD
    ));


    public int getBucketTrafficStatistics(MsHttpRequest request) {
        String accountId = request.getUserId();
        MsAclUtils.checkIfAnonymous(accountId);
        String startTime = request.getParam(MGT_STAT_START_TIME);
        String endTime = request.getParam(MGT_STAT_END_TIME);
        String requestsType = request.getParam(MGT_STAT_REQ_TYPE).toLowerCase();
        String bucketName = request.getBucketName();
        String[] bucketCtime = new String[1];

        checkTrafficStatisticsParam(startTime, endTime, requestsType);
        long startStamp = MsDateUtils.standardDateToStamp(startTime);
        long endStamp = MsDateUtils.standardDateToStamp(endTime);
        if (startStamp > endStamp) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "Time param error, startTime must <= endTime when get traffic statistics.");
        }
        String standardRequestType = mapToStandardRequestType(requestsType);
        final String accountVnode = STATISTIC_STORAGE_POOL.getBucketVnodeId(accountId);
        StatisticsRecorder.StatisticRecord resultRecord0 = new StatisticsRecorder.StatisticRecord();
        Disposable subscribe = pool.getReactive(REDIS_BUCKETINFO_INDEX).hgetall(bucketName)
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo, new MsException(NO_SUCH_BUCKET, "No such bucket")))  // 判断桶是否存在
                .doOnNext(bucketInfo -> {
                    String bucketUserId = bucketInfo.getOrDefault("user_id", "The account has no permission to operate this bucket");
                    if (!bucketUserId.equals(accountId)) {
                        throw new MsException(ErrorNo.ACCESS_FORBIDDEN, "The account has no permission to operate this bucket");
                    }
                }) // 判断id是否一致
                .flatMap(b -> pool.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucketName, "ctime"))
                .flatMap(s -> {
                    bucketCtime[0] = s;
                    return STATISTIC_STORAGE_POOL.mapToNodeInfo(accountVnode);
                })
                .flatMapMany(nodelist -> {
                    long realStartStamp = startStamp;
                    // 如果创桶时间在开始时间之后，替换查询的起始时间为创桶时间
                    if (Long.parseLong(bucketCtime[0]) > startStamp) {
                        realStartStamp = Long.parseLong(bucketCtime[0]);
                    }
                    HashMap<String, Object> msgData = new HashMap<>(8);
                    msgData.put(MGT_STAT_START_STAMP, realStartStamp);
                    msgData.put(MGT_STAT_END_STAMP, endStamp);
                    msgData.put(MGT_STAT_REQUEST_TYPE, standardRequestType);
                    msgData.put(MGT_STAT_ACCOUNT, accountId);
                    msgData.put(MGT_STAT_BUCKET, request.getBucketName());
                    msgData.put("vnode", accountVnode);
                    List<SocketReqMsg> socketReqMsgs = ECUtils.mapToMsg("", Json.encode(msgData), nodelist);
                    return listRecord(accountVnode, accountId, socketReqMsgs, nodelist);
                })
                .defaultIfEmpty(new StatisticsRecorder.StatisticRecord())
                .subscribe(resultRecord0::mergeRecord, e -> MsException.dealException(request, e), () -> {
                    TrafficStatisticsResult trafficStatisticsResult = buildTrafficStatisticsResponse(startStamp, endStamp, requestsType, resultRecord0);

                    byte[] initRes = JaxbUtils.toByteArray(trafficStatisticsResult);
                    addPublicHeaders(request, RequestBuilder.getRequestId())
                            .putHeader(CONTENT_TYPE, XML_RESPONSE)
                            .putHeader(CONTENT_LENGTH, String.valueOf(initRes.length))
                            .write(Buffer.buffer(initRes));
                    addAllowHeader(request.response()).end();
                });

        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
        return 0;
    }

    private Flux<StatisticsRecorder.StatisticRecord> listRecord(String accountVnode, String accountId, List<SocketReqMsg> socketReqMsgs, List<Tuple3<String, String, String>> nodelist) {
        UnicastProcessor<StatisticsRecorder.StatisticRecord> recordFlux = UnicastProcessor.create();
        UnicastProcessor<String> listController = UnicastProcessor.create();
        listController.subscribe(s -> {
            listRecord0(s, socketReqMsgs, nodelist, recordFlux, listController);
        });
        listController.onNext("");

        return recordFlux;
    }

    private void listRecord0(String maker, List<SocketReqMsg> socketReqMsgs, List<Tuple3<String, String, String>> nodelist,
                             UnicastProcessor<StatisticsRecorder.StatisticRecord> listFlux, UnicastProcessor<String> listController) {
        List<SocketReqMsg> socketReqMsgList = socketReqMsgs.stream().map(socketReqMsg ->
                socketReqMsg.put("maker", maker)).collect(Collectors.toList());

        ClientTemplate.ResponseInfo<StatisticsRecorder.StatisticRecord[]> listResponseInfo = ClientTemplate
                .oneResponse(socketReqMsgList, GET_TRAFFIC_STATISTICS, new TypeReference<StatisticsRecorder.StatisticRecord[]>() {
                }, nodelist);
        ArrayList<StatisticsRecorder.StatisticRecord> statisticRecords = new ArrayList<>();
        ListRecodeHandler listRecodeHandler = new ListRecodeHandler(listResponseInfo, nodelist, statisticRecords, maker);
        listResponseInfo.responses
                .doOnNext(listRecodeHandler::handleResponse)
                .doOnComplete(listRecodeHandler::handleComplete)
                .count()
                .flatMap(x -> listRecodeHandler.res)
                .subscribe(b -> {
                    if (b) {
                        if (listRecodeHandler.count <= 0) {
                            listFlux.onComplete();
                        } else {
                            statisticRecords.forEach(listFlux::onNext);
                            listController.onNext(listRecodeHandler.marker);
                        }
                    } else {
                        listFlux.onError(new MsException(UNKNOWN_ERROR, ""));
                    }
                });
    }

    /**
     * 获取指定用户某个时间段内统计信息
     * 包括：读请求次数，写请求次数，总请求次数，写流量大小，读流量大小，访问总流量大小
     *
     * @param request 请求对象
     * @return 返回的数字没意义，会向传入的request中写入response
     */
    public int getTrafficStatistics(MsHttpRequest request) {
        String accountId = request.getUserId();
        if (request.getMember("system") == null) {
            if ("admin".equals(accountId) && request.headers().contains("accountId")) {
                accountId = request.headers().get("accountId");
            }
            MsAclUtils.checkIfAnonymous(accountId);
        } else {
            accountId = StatisticsRecorder.SYSTEM_RECORD_ACCOUNT_ID;
        }

        String startTime = request.getParam(MGT_STAT_START_TIME);
        String endTime = request.getParam(MGT_STAT_END_TIME);
        String requestsType = request.getParam(MGT_STAT_REQ_TYPE).toLowerCase();
        checkTrafficStatisticsParam(startTime, endTime, requestsType);
        long startStamp = MsDateUtils.standardDateToStamp(startTime);
        long endStamp = MsDateUtils.standardDateToStamp(endTime);
        if (startStamp > endStamp) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "Time param error, startTime must <= endTime when get traffic statistics.");
        }
        String standardRequestType = mapToStandardRequestType(requestsType);
        final String accountVnode = STATISTIC_STORAGE_POOL.getBucketVnodeId(accountId);
        StatisticsRecorder.StatisticRecord resultRecord0 = new StatisticsRecorder.StatisticRecord();

        String finalAccountId = accountId;
        Disposable subscribe = STATISTIC_STORAGE_POOL.mapToNodeInfo(accountVnode)
                .flatMapMany(nodelist -> {
                    HashMap<String, Object> msgData = new HashMap<>(8);
                    msgData.put(MGT_STAT_START_STAMP, startStamp);
                    msgData.put(MGT_STAT_END_STAMP, endStamp);
                    msgData.put(MGT_STAT_REQUEST_TYPE, standardRequestType);
                    msgData.put(MGT_STAT_ACCOUNT, finalAccountId);
                    msgData.put(MGT_STAT_BUCKET, request.getBucketName());
                    msgData.put("vnode", accountVnode);
                    List<SocketReqMsg> socketReqMsgs = ECUtils.mapToMsg("", Json.encode(msgData), nodelist);
                    return listRecord(accountVnode, finalAccountId, socketReqMsgs, nodelist);
                })
                .defaultIfEmpty(new StatisticsRecorder.StatisticRecord())
                .subscribe(resultRecord0::mergeRecord, e -> MsException.dealException(request, e), () -> {
                    TrafficStatisticsResult trafficStatisticsResult = buildTrafficStatisticsResponse(startStamp, endStamp, requestsType, resultRecord0);

                    byte[] initRes = JaxbUtils.toByteArray(trafficStatisticsResult);
                    addPublicHeaders(request, RequestBuilder.getRequestId())
                            .putHeader(CONTENT_TYPE, XML_RESPONSE)
                            .putHeader(CONTENT_LENGTH, String.valueOf(initRes.length))
                            .write(Buffer.buffer(initRes));
                    addAllowHeader(request.response()).end();
                });

        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
        return 0;
    }

    /**
     * 获取某个时间段内统计信息
     * 包括：读请求次数，写请求次数，总请求次数，写流量大小，读流量大小，访问总流量大小
     *
     * @param request 请求对象
     * @return 返回的数字没意义，会向传入的request中写入response
     */
    public int getTrafficStatisticsList(MsHttpRequest request) {
        String accountId = "admin".equals(request.getUserId()) && request.headers().contains("accountId") ? request.headers().get("accountId") : request.getUserId();
        MsAclUtils.checkIfAnonymous(accountId);
        String startTime = request.getParam(MGT_STAT_START_TIME);
        String endTime = request.getParam(MGT_STAT_END_TIME);
        boolean respectively = "respectively".equals(request.getParam(MGT_STAT_REQ_TYPE).toLowerCase());
        String requestsType = respectively ? "read" : request.getParam(MGT_STAT_REQ_TYPE).toLowerCase();

        checkTrafficStatisticsParam(startTime, endTime, requestsType);
        long startStamp = MsDateUtils.standardDateToStamp(startTime);
        long endStamp = MsDateUtils.standardDateToStamp(endTime);
        if (startStamp > endStamp) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "Time param error, startTime must <= endTime when get traffic statistics.");
        }
        String[] bucketCtime = new String[1];
        final String accountVnode = STATISTIC_STORAGE_POOL.getBucketVnodeId(accountId);
        TrafficStatisticsResultList result = new TrafficStatisticsResultList();
        List<TrafficStatisticsResult> resultList = new ArrayList<>();
        Disposable subscribe = pool.getReactive(REDIS_USERINFO_INDEX).hgetall(accountId)
                .doOnNext(info -> throwWhenEmpty(info, new MsException(ErrorNo.NO_SUCH_ACCOUNT, "no such account. account id :" + accountId + ".")))
                .flatMapMany(b -> pool.getReactive(REDIS_USERINFO_INDEX).smembers(accountId + USER_BUCKET_SET_SUFFIX))
                .flatMap(bucketName -> {
                    StatisticsRecorder.StatisticRecord resultRecord0 = new StatisticsRecorder.StatisticRecord();
                    StatisticsRecorder.StatisticRecord resultRecord1 = new StatisticsRecorder.StatisticRecord();
                    return pool.getReactive(REDIS_BUCKETINFO_INDEX).hget(bucketName, "ctime")
                            .flatMap(s -> {
                                bucketCtime[0] = s;
                                return STATISTIC_STORAGE_POOL.mapToNodeInfo(accountVnode);
                            })
                            .flatMapMany(nodelist -> {
                                long realStartStamp = startStamp;
                                // 如果创桶时间在开始时间之后，替换查询的起始时间为创桶时间
                                if (Long.parseLong(bucketCtime[0]) > startStamp) {
                                    realStartStamp = Long.parseLong(bucketCtime[0]);
                                }
                                HashMap<String, Object> msgData = new HashMap<>(8);
                                msgData.put(MGT_STAT_START_STAMP, realStartStamp);
                                msgData.put(MGT_STAT_END_STAMP, endStamp);
                                msgData.put(MGT_STAT_REQUEST_TYPE, respectively ? requestsType : mapToStandardRequestType(requestsType));
                                msgData.put(MGT_STAT_ACCOUNT, accountId);
                                msgData.put(MGT_STAT_BUCKET, bucketName);
                                msgData.put("vnode", accountVnode);
                                List<SocketReqMsg> socketReqMsgs = ECUtils.mapToMsg("", Json.encode(msgData), nodelist);
                                return listRecord(accountVnode, accountId, socketReqMsgs, nodelist);
                            })
                            .defaultIfEmpty(new StatisticsRecorder.StatisticRecord())
                            .doOnNext(resultRecord0::mergeRecord)
                            .collectList()
                            .flatMap(a -> {
                                if (respectively) {
                                    return STATISTIC_STORAGE_POOL.mapToNodeInfo(accountVnode)
                                            .flatMapMany(nodelist -> {
                                                long realStartStamp = startStamp;
                                                // 如果创桶时间在开始时间之后，替换查询的起始时间为创桶时间
                                                if (Long.parseLong(bucketCtime[0]) > startStamp) {
                                                    realStartStamp = Long.parseLong(bucketCtime[0]);
                                                }
                                                HashMap<String, Object> msgData = new HashMap<>(8);
                                                msgData.put(MGT_STAT_START_STAMP, realStartStamp);
                                                msgData.put(MGT_STAT_END_STAMP, endStamp);
                                                msgData.put(MGT_STAT_REQUEST_TYPE, "write");
                                                msgData.put(MGT_STAT_ACCOUNT, accountId);
                                                msgData.put(MGT_STAT_BUCKET, bucketName);
                                                msgData.put("vnode", accountVnode);
                                                List<SocketReqMsg> socketReqMsgs = ECUtils.mapToMsg("", Json.encode(msgData), nodelist);
                                                return listRecord(accountVnode, accountId, socketReqMsgs, nodelist);
                                            })
                                            .defaultIfEmpty(new StatisticsRecorder.StatisticRecord())
                                            .doOnNext(statisticRecord -> {
                                                resultRecord1.mergeRecord(statisticRecord);
                                            })
                                            .collectList();
                                } else {
                                    return Mono.just(1);
                                }
                            })
                            .doOnNext(record -> {
                                TrafficStatisticsResult trafficStatisticsResult;
                                if (respectively) {
                                    trafficStatisticsResult = new TrafficStatisticsResult()
                                            .setStartTime(MsDateUtils.stampToISO8601(startStamp))
                                            .setEndTime(MsDateUtils.stampToISO8601(endStamp));
                                    trafficStatisticsResult.setReadTraffic(String.valueOf(resultRecord0.getTraffic()));
                                    trafficStatisticsResult.setReadCount(String.valueOf(resultRecord0.getCount()));
                                    trafficStatisticsResult.setWriteTraffic(String.valueOf(resultRecord1.getTraffic()));
                                    trafficStatisticsResult.setWriteCount(String.valueOf(resultRecord1.getCount()));
                                    trafficStatisticsResult.setTotalTime(String.valueOf(resultRecord0.getTotalTime() + resultRecord1.getTotalTime()));
                                    List<HttpMethodTrafficStatisticsResult> allHttpMethodTrafficStatisticsResult = resultRecord0.getAllHttpMethodTrafficStatisticsResult();
                                    allHttpMethodTrafficStatisticsResult.addAll(resultRecord1.getAllHttpMethodTrafficStatisticsResult());
                                    trafficStatisticsResult.setHttpMethodTrafficStatisticsResult(allHttpMethodTrafficStatisticsResult);
                                } else {
                                    trafficStatisticsResult = buildTrafficStatisticsResponse(startStamp, endStamp, requestsType, resultRecord0);
                                }
                                trafficStatisticsResult.setBucketName(bucketName);
                                resultList.add(trafficStatisticsResult);
                            });
                })
                .subscribe(recordList -> {
                        },
                        e -> MsException.dealException(request, e),
                        () -> {
                            result.setTrafficStatisticsResultList(resultList);
                            byte[] initRes = JaxbUtils.toByteArray(result);
                            addPublicHeaders(request, RequestBuilder.getRequestId())
                                    .putHeader(CONTENT_TYPE, XML_RESPONSE)
                                    .putHeader(CONTENT_LENGTH, String.valueOf(initRes.length))
                                    .write(Buffer.buffer(initRes));
                            addAllowHeader(request.response()).end();
                        });

        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
        return 0;
    }


    /**
     * 获取系统某个时间段内统计信息
     * 包括：读请求次数，写请求次数，总请求次数，写流量大小，读流量大小，访问总流量大小
     *
     * @param request 请求对象
     * @return 返回的数字没意义，会向传入的request中写入response
     */
    public int getSystemTrafficStatistics(MsHttpRequest request) {
        String userId = request.getUserId();
        MsAclUtils.checkIfManageAccount(userId);
        request.addMember("system", "true");
        return getTrafficStatistics(request);
    }


    /**
     * 获取当前账户下对象统计信息
     * 包括：对象总个数、存储总容量
     *
     * @param request 请求对象
     * @return 返回的数字没意义，会向传入的request中写入response
     */
    public int getObjectStatistics(MsHttpRequest request) {
        String accountId = request.getUserId();
        // 根据传过来的账户参数获取账户名称
        Disposable subscribe = pool.getReactive(REDIS_USERINFO_INDEX).hget(accountId, USER_DATABASE_ID_NAME)
                .doOnNext(accountName -> MsAclUtils.checkIfAnonymous(accountId))
                .flatMap(accountName -> QuotaRecorder.getAccountTotalStorage(accountName).zipWith(QuotaRecorder.getAccountTotalObject(accountName)))
                .subscribe(tuple2 -> {
                    try {
                        ObjectStatisticsResult objectStatisticsResult = new ObjectStatisticsResult()
                                .setStorageCapacityUsed(tuple2.getT1().toString())
                                .setObjectCount(tuple2.getT2().toString());
                        byte[] data = JaxbUtils.toByteArray(objectStatisticsResult);
                        addPublicHeaders(request, RequestBuilder.getRequestId())
                                .putHeader(CONTENT_TYPE, XML_RESPONSE)
                                .putHeader(CONTENT_LENGTH, String.valueOf(data.length))
                                .write(Buffer.buffer(data));
                        addAllowHeader(request.response()).end();
                    } catch (Exception e) {
                        MsException.dealException(request, e);
                    }
                }, e -> MsException.dealException(request, e));
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
        return ErrorNo.SUCCESS_STATUS;
    }

    /**
     * 判断流量统计参数有效性
     *
     * @param startTime   YYYY-MM-DD HH:MM:SS
     * @param endTime     YYYY-MM-DD HH:MM:SS
     * @param requestType read|write|total|get|delete|put|post
     */
    private static void checkTrafficStatisticsParam(String startTime, String endTime, String requestType) {
        if (StringUtils.isEmpty(startTime) || StringUtils.isEmpty(endTime) || StringUtils.isEmpty(requestType)) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "Set requestType invalid when get traffic statistics.");
        }
        if (!REQUEST_TYPES.contains(requestType)) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "Set requestType invalid when get traffic statistics.");
        }
        checkTimePattern(startTime);
        checkTimePattern(endTime);
    }

    /**
     * 检查时间格式有效性
     *
     * @param time YYYY-MM-DD HH:MM:SS
     */
    private static void checkTimePattern(String time) {
        if (!PatternConst.TRAFFIC_STATIS_TIME_PATTERN.matcher(time).matches()) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "Time param must set YYYY-MM-DD HH:MM:SS when get traffic statistics.");
        }
    }


    private static TrafficStatisticsResult buildTrafficStatisticsResponse(long startStamp, long endStamp, String requestsType, StatisticsRecorder.StatisticRecord resultRecord0) {
        TrafficStatisticsResult trafficStatisticsResult = new TrafficStatisticsResult()
                .setStartTime(MsDateUtils.stampToISO8601(startStamp))
                .setEndTime(MsDateUtils.stampToISO8601(endStamp));
        switch (requestsType) {
            case MGT_STAT_REQ_TYPE_READ: {
                trafficStatisticsResult.setReadTraffic(String.valueOf(resultRecord0.getTraffic()));
                trafficStatisticsResult.setReadCount(String.valueOf(resultRecord0.getCount()));
                trafficStatisticsResult.setTotalTime(String.valueOf(resultRecord0.getTotalTime()));
                List<HttpMethodTrafficStatisticsResult> allHttpMethodTrafficStatisticsResult = resultRecord0.getAllHttpMethodTrafficStatisticsResult();
                if (!allHttpMethodTrafficStatisticsResult.isEmpty()) {
                    trafficStatisticsResult.setHttpMethodTrafficStatisticsResult(allHttpMethodTrafficStatisticsResult);
                }
                break;
            }
            case MGT_STAT_REQ_TYPE_WRITE: {
                trafficStatisticsResult.setWriteTraffic(String.valueOf(resultRecord0.getTraffic()));
                trafficStatisticsResult.setWriteCount(String.valueOf(resultRecord0.getCount()));
                trafficStatisticsResult.setTotalTime(String.valueOf(resultRecord0.getTotalTime()));
                List<HttpMethodTrafficStatisticsResult> allHttpMethodTrafficStatisticsResult = resultRecord0.getAllHttpMethodTrafficStatisticsResult();
                if (!allHttpMethodTrafficStatisticsResult.isEmpty()) {
                    trafficStatisticsResult.setHttpMethodTrafficStatisticsResult(allHttpMethodTrafficStatisticsResult);
                }
                break;
            }
            case MGT_STAT_REQ_TYPE_TOTAL: {
                trafficStatisticsResult.setTotalTraffic(String.valueOf(resultRecord0.getTraffic()));
                trafficStatisticsResult.setTotalCount(String.valueOf(resultRecord0.getCount()));
                trafficStatisticsResult.setTotalTime(String.valueOf(resultRecord0.getTotalTime()));
                List<HttpMethodTrafficStatisticsResult> allHttpMethodTrafficStatisticsResult = resultRecord0.getAllHttpMethodTrafficStatisticsResult();
                if (!allHttpMethodTrafficStatisticsResult.isEmpty()) {
                    trafficStatisticsResult.setHttpMethodTrafficStatisticsResult(allHttpMethodTrafficStatisticsResult);
                }
                break;
            }
            case MGT_STAT_REQ_TYPE_GET: {
                List<HttpMethodTrafficStatisticsResult> singleHttpMethodTrafficStatisticsResult = resultRecord0.getSingleHttpMethodTrafficStatisticsResult(MGT_STAT_REQ_TYPE_GET);
                if (!singleHttpMethodTrafficStatisticsResult.isEmpty()) {
                    trafficStatisticsResult.setHttpMethodTrafficStatisticsResult(singleHttpMethodTrafficStatisticsResult);
                }
                break;
            }
            case MGT_STAT_REQ_TYPE_DELETE: {
                List<HttpMethodTrafficStatisticsResult> singleHttpMethodTrafficStatisticsResult = resultRecord0.getSingleHttpMethodTrafficStatisticsResult(MGT_STAT_REQ_TYPE_DELETE);
                if (!singleHttpMethodTrafficStatisticsResult.isEmpty()) {
                    trafficStatisticsResult.setHttpMethodTrafficStatisticsResult(singleHttpMethodTrafficStatisticsResult);
                }
                break;
            }
            case MGT_STAT_REQ_TYPE_PUT: {
                List<HttpMethodTrafficStatisticsResult> singleHttpMethodTrafficStatisticsResult = resultRecord0.getSingleHttpMethodTrafficStatisticsResult(MGT_STAT_REQ_TYPE_PUT);
                if (!singleHttpMethodTrafficStatisticsResult.isEmpty()) {
                    trafficStatisticsResult.setHttpMethodTrafficStatisticsResult(singleHttpMethodTrafficStatisticsResult);
                }
                break;
            }
            case MGT_STAT_REQ_TYPE_POST: {
                List<HttpMethodTrafficStatisticsResult> singleHttpMethodTrafficStatisticsResult = resultRecord0.getSingleHttpMethodTrafficStatisticsResult(MGT_STAT_REQ_TYPE_POST);
                if (!singleHttpMethodTrafficStatisticsResult.isEmpty()) {
                    trafficStatisticsResult.setHttpMethodTrafficStatisticsResult(singleHttpMethodTrafficStatisticsResult);
                }
                break;
            }
            case MGT_STAT_REQ_TYPE_HEAD: {
                List<HttpMethodTrafficStatisticsResult> singleHttpMethodTrafficStatisticsResult = resultRecord0.getSingleHttpMethodTrafficStatisticsResult(MGT_STAT_REQ_TYPE_HEAD);
                if (!singleHttpMethodTrafficStatisticsResult.isEmpty()) {
                    trafficStatisticsResult.setHttpMethodTrafficStatisticsResult(singleHttpMethodTrafficStatisticsResult);
                }
            }
        }
        return trafficStatisticsResult;
    }

    /**
     * 将用户传入的请求类型映射为标准请求类型
     * 标准请求类型：read、write、total
     *
     * @param requestType 用户传入的请求类型
     * @return 标准请求类型
     */
    private static String mapToStandardRequestType(String requestType) {
        switch (requestType) {
            case MGT_STAT_REQ_TYPE_GET:
            case MGT_STAT_REQ_TYPE_HEAD:
            case MGT_STAT_REQ_TYPE_READ:
                return MGT_STAT_REQ_TYPE_READ;
            case MGT_STAT_REQ_TYPE_DELETE:
            case MGT_STAT_REQ_TYPE_PUT:
            case MGT_STAT_REQ_TYPE_POST:
            case MGT_STAT_REQ_TYPE_WRITE:
                return MGT_STAT_REQ_TYPE_WRITE;
            case MGT_STAT_REQ_TYPE_TOTAL:
                return MGT_STAT_REQ_TYPE_TOTAL;
            default:
                throw new MsException(ErrorNo.INVALID_ARGUMENT, "Set requestType invalid when get traffic statistics.");
        }
    }

}