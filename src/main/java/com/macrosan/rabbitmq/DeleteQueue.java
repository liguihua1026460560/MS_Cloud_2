package com.macrosan.rabbitmq;/**
 * @author niechengxing
 * @create 2024-04-24 16:00
 */

import com.rabbitmq.client.Channel;
import lombok.Data;

/**
 *@program: MS_Cloud
 *@description: 封装每个需要删除的旧队列信息
 *@author: niechengxing
 *@create: 2024-04-24 16:00
 */
@Data
public class DeleteQueue {
    Channel channel;
    String queue;
    public DeleteQueue(Channel channel, String queueName) {
        this.channel = channel;
        this.queue = queueName;
    }
}

