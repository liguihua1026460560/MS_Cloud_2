/**
 * Copyright (C), 2019-2020,杭州宏杉科技股份有限公司
 * FileName: ListRecodeHandler
 * Author: xiangzicheng-PC
 * Date: 2020/5/29 10:23
 * Description: 处理从其他服务器获取到的一定时间段内record
 * History:
 */

package com.macrosan.storage.client;

import com.macrosan.ec.server.ErasureServer;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.quota.StatisticsRecorder;
import reactor.core.publisher.Mono;

import java.util.List;

import static com.macrosan.utils.quota.StatisticsRecorder.STATISTIC_STORAGE_POOL;

/**
 * 处理从其他服务器获取到的一定时间段内record
 *
 * @author xiangzicheng-PC
 * @create 2020/5/29
 * @since 1.0.0
 */
public class ListRecodeHandler extends AbstractListClient<StatisticsRecorder.StatisticRecord> {
    public List<StatisticsRecorder.StatisticRecord> recordResult;
    public int count = 0;
    public String marker;

    public ListRecodeHandler(ClientTemplate.ResponseInfo<StatisticsRecorder.StatisticRecord[]> responseInfo, List<Tuple3<String, String, String>> nodeList,
                             List<StatisticsRecorder.StatisticRecord> recordResult, String maker) {
        super(STATISTIC_STORAGE_POOL, responseInfo, nodeList);
        this.recordResult = recordResult;
        this.marker = maker;
    }

    public ListRecodeHandler(ClientTemplate.ResponseInfo<StatisticsRecorder.StatisticRecord[]> responseInfo, List<Tuple3<String, String, String>> nodeList,
                             List<StatisticsRecorder.StatisticRecord> recordResult, MsHttpRequest request) {
        super(STATISTIC_STORAGE_POOL, responseInfo, nodeList, request);
        this.recordResult = recordResult;
    }

    @Override
    protected void publishResult() {
        this.res.onNext(true);
    }

    @Override
    protected String getKey(StatisticsRecorder.StatisticRecord statisticRecord) {
        return statisticRecord.getKey();
    }

    @Override
    protected int compareTo(StatisticsRecorder.StatisticRecord t1, StatisticsRecorder.StatisticRecord t2) {
        if (t2.getVersionNum().equals(t1.getVersionNum())) {
            return 0;
        } else {
            //versionNum不一致最新versionNum为较小值
            return t1.getVersionNum().compareTo(t2.getVersionNum());
        }
    }

    @Override
    protected void handleResult(StatisticsRecorder.StatisticRecord statisticRecord) {
        this.recordResult.add(statisticRecord);
        count ++;
    }

    @Override
    protected Mono<Boolean> repair(Counter counter, List<Tuple3<String, String, String>> nodeList) {
        //todo
        return Mono.just(true);
    }

    @Override
    protected void putErrorList(Counter counter) {

    }

    @Override
    protected void publishErrorList() {
    }

    @Override
    public void handleResponse(Tuple3<Integer, ErasureServer.PayloadMetaType, StatisticsRecorder.StatisticRecord[]> tuple) {
        if (null != tuple.var3 && tuple.var3.length > 0) {
            marker = tuple.var3[tuple.var3.length - 1].getKey();
        }
        super.handleResponse(tuple);
    }
}