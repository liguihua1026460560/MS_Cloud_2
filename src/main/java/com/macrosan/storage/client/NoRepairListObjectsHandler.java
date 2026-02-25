package com.macrosan.storage.client;

import com.macrosan.ec.Utils;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.storage.StoragePool;
import com.macrosan.utils.functional.Tuple3;
import reactor.core.publisher.Mono;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class NoRepairListObjectsHandler extends AbstractListClient<Tuple3<Boolean, String, MetaData>> {

    public final LinkedList<Tuple3<Boolean, String, MetaData>> list = new LinkedList<>();

    public NoRepairListObjectsHandler(StoragePool pool, ClientTemplate.ResponseInfo<Tuple3<Boolean, String, MetaData>[]> responseInfo, List<Tuple3<String, String, String>> nodeList) {
        super(pool, responseInfo, nodeList);
    }


    @Override
    protected void publishResult() {
        res.onNext(true);
    }

    @Override
    protected String getKey(Tuple3<Boolean, String, MetaData> t) {
        return Utils.getMetaDataKey(vnode, t.var3.getBucket(), t.var3.getKey(), null);
    }

    @Override
    protected int compareTo(Tuple3<Boolean, String, MetaData> t1, Tuple3<Boolean, String, MetaData> t2) {
        if (!Objects.equals(t1.var3.snapshotMark, t2.var3.snapshotMark)) {
            // 同名对象不是一个快照下的，则当前快照标记为最新对象
            return t1.var3.snapshotMark.equals(currentSnapshotMark) ? 1 : -1;
        }
        if (t2.var3.versionNum.equals(t1.var3.versionNum)) {
            // versionNum一致按key排序
            return t2.var3.key.compareTo(t1.var3.key);
        } else {
            //versionNum不一致最新versionNum为较小值
            return t1.var3.versionNum.compareTo(t2.var3.versionNum);
        }
    }


    @Override
    protected void handleResult(Tuple3<Boolean, String, MetaData> t) {
        MetaData metaData = t.var3;
        if (!metaData.deleteMark) {
            list.add(t);
        }
    }

    @Override
    protected Mono<Boolean> repair(AbstractListClient<Tuple3<Boolean, String, MetaData>>.Counter counter, List<Tuple3<String, String, String>> nodeList) {
        // nothing to do
        return Mono.just(true);
    }

    @Override
    protected void putErrorList(AbstractListClient<Tuple3<Boolean, String, MetaData>>.Counter counter) {
        // nothing to do
    }

    @Override
    protected void publishErrorList() {
        // nothing to do
    }
}
