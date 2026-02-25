package com.macrosan.utils.listutil.context;

import com.macrosan.utils.listutil.ListOptions;
import com.macrosan.utils.listutil.interpcy.InterruptPolicy;

public interface ListOperationContext<T> {
    String getBucket();
    String getPrefix();
    InterruptPolicy<T> getInterruptPolicy();
    ListOptions getListOptions();
}
