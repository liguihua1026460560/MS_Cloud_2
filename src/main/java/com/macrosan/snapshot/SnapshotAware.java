package com.macrosan.snapshot;

import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * @author zhaoyang
 * @date 2024/07/15
 * @description 扩展元数据实体类，提供快照相关的公共方法
 **/
public interface SnapshotAware<T> {

    T setUnView(Set<String> unView);

    Set<String> getUnView();

    T setSnapshotMark(String mark);

    String getSnapshotMark();


    default boolean isUnView(String mark) {
        return getUnView() != null && getUnView().contains(mark);
    }


    default boolean isViewable(String mark) {
        return !isUnView(mark);
    }


    /**
     * 为对象元数据添加 不可见的快照标记
     *
     * @param mark 不可见的快照标记
     */
    default void addUnViewSnapshotMark(String mark) {
        Set<String> unView = new HashSet<>(1);
        unView.add(mark);
        setUnView(unView);
    }


    /**
     * 将快照标记从不可见列表中移除
     *
     * @param mark 待移除标记
     */
    default void removeUnViewSnapshotMark(String mark) {
        Optional.ofNullable(getUnView())
                .ifPresent(unView -> {
                    getUnView().remove(mark);
                    if (getUnView().isEmpty()) {
                        setUnView(null);
                    }
                });
    }

    /**
     * 判断该元数据是否为当前快照下上传的
     *
     * @param currentSnapshotMark 当前快照标识
     * @return 如果为当前快照下上传的返回true
     */
    default boolean isCurrentSnapshotObject(String currentSnapshotMark) {
        return Objects.equals(getSnapshotMark(), currentSnapshotMark);
    }
}