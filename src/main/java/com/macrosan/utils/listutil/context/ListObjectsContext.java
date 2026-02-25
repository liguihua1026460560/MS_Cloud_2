package com.macrosan.utils.listutil.context;

import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.utils.listutil.ListOptions;
import com.macrosan.utils.listutil.interpcy.InterruptPolicy;

public class ListObjectsContext implements ListOperationContext<MetaData> {
    private final String bucket;
    private final String prefix;
    private final InterruptPolicy<MetaData> policy;
    private final ListOptions options;

    public ListObjectsContext(String bucket, String prefix, ListOptions options) {
        this(bucket, prefix, null, new ListOptions());
    }

    public ListObjectsContext(String bucket, String prefix, InterruptPolicy<MetaData> policy, ListOptions options) {
        this.bucket = bucket;
        this.prefix = prefix;
        this.policy = policy;
        this.options = options;
    }

    @Override
    public String getBucket() {
        return bucket;
    }

    @Override
    public String getPrefix() {
        return prefix;
    }

    @Override
    public InterruptPolicy<MetaData> getInterruptPolicy() {
        return policy;
    }

    @Override
    public ListOptions getListOptions() {
        return options;
    }
}
