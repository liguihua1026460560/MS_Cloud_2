package com.macrosan.storage.strategy;

import com.macrosan.storage.strategy.select.*;

/**
 * @author gaozhiyuan
 */
abstract class SelectorStrategy extends StorageStrategy {
    Selector selector;

    SelectorStrategy(SelectStrategy selectStrategy, String... args) {
        if (null == selectStrategy) {
            this.selector = new HealthSelector(1.0, 0L);
            return;
        }

        switch (selectStrategy) {
            case NO:
                this.selector = new NoSelector();
                break;
            case RANDOM:
                this.selector = new RandomSelector();
                break;
            case SIZE:
                if (args.length >= 2) {
                    double rate = Double.parseDouble(args[0]);
                    long size = Long.parseLong(args[1]);
                    this.selector = new SizeSelector(rate, size);
                } else {
                    this.selector = new SizeSelector(1.0, 0L);
                }
                break;
            case HEALTH:
            default:
                if (args.length >= 2) {
                    double rate = Double.parseDouble(args[0]);
                    long size = Long.parseLong(args[1]);
                    this.selector = new HealthSelector(rate, size);
                } else {
                    this.selector = new HealthSelector(1.0, 0L);
                }
        }
    }

}
