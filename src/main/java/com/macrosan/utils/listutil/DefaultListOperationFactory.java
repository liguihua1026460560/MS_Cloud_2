package com.macrosan.utils.listutil;

import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.utils.listutil.context.ListOperationContext;
import com.macrosan.utils.listutil.interpcy.InterruptPolicy;

public class DefaultListOperationFactory {

    @SuppressWarnings("unchecked")
    public static <T> ListOperation<T> create(ListType type, ListOperationContext<T> context) {
        switch (type) {
            case LSFILE:
                return (ListOperation<T>) new ListCurrentObjectOperation(
                        context.getBucket(),
                        context.getPrefix(),
                        (InterruptPolicy<MetaData>) context.getInterruptPolicy(),
                        context.getListOptions()
                );
            case LSVERSIONS:
                return (ListOperation<T>) new ListVersionsObjectOperation(
                        context.getBucket(),
                        context.getPrefix(),
                        (InterruptPolicy<MetaData>) context.getInterruptPolicy(),
                        context.getListOptions()
                );
            default:
                throw new IllegalArgumentException("Unsupported list type: " + type);
        }
    }
}
