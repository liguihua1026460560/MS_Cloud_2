package com.macrosan.rabbitmq;

import com.macrosan.ec.error.ErrorConstant;
import com.macrosan.ec.rebuild.DiskStatusChecker;
import com.macrosan.ec.rebuild.RemovedDisk;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.macrosan.rabbitmq.RabbitMqChannels.initChannelByIp;
import static com.macrosan.rabbitmq.RabbitMqUtils.*;
import static com.macrosan.rabbitmq.RabbitMqUtils.ERROR_MQ_TYPE.*;

/**
 * EC流程中object相关的消息队列发布者
 *
 * @author fanjunxi
 */
@Log4j2
public class ObjectPublisher {


    /**
     * 一般的异常消息发布。会根据磁盘状态发布到一般队列或缓冲队列
     *
     * @param dstIp 该消息应该由哪个节点的前端jar去消费。dstIp和msg中的lun是绑定的。
     *              实际上publish还是在本地的queue进行，该队列和dstIp上的消费者绑定
     * @注意 与对象或者存储池相关的修复消息请在消息内容中携带poolQueueTag信息(value为相关的存储池名称)用于队列的分发，不携带则不会分发至存储池相关队列
     */
    public static void publish(String dstIp, SocketReqMsg msg, ErrorConstant.ECErrorType errorType) {
        publish(dstIp, msg, errorType, true);
    }

    public static void publish(String dstIp, SocketReqMsg msg, ErrorConstant.ECErrorType errorType, boolean needCheck) {
        try {
            String diskName = msg.get("lun");

            if (checkMsgCanPublish(diskName, dstIp)) {
                diskName = diskName.contains("@") ? diskName.split("#")[0] : getDiskName(dstIp, diskName);
                if (StringUtils.isNotBlank(msg.get("lun"))) {
                    String lun;
                    if (diskName.contains("@")) {
                        String[] split = diskName.split("@");
                        lun = split.length > 1 ? split[1] : split[0];
                    } else {
                        lun = diskName;
                    }
                    if (DiskStatusChecker.isRebuildWaiter(lun) || RemovedDisk.getInstance().contains(diskName)) {
                        // 磁盘故障 不进行publish
                        return;
                    }
                }
                //目标盘可用，publish到一般队列，计数；不可用，publish到缓冲队列
                if (diskIsAvailable(diskName)) {
                    basicPublish(EC_ERROR, dstIp, msg, errorType);
                    if (diskName != null && needCheck) {
                        addErrorMsgAmount(diskName);
                    }
                } else {
                    basicPublish(EC_BUFFERED_ERROR, dstIp, msg, errorType);
                }
            }
        } catch (Exception e) {
            log.error("publish {} to rabbit mq error. {}", errorType, e);
        }
    }

    public static boolean checkMsgCanPublish(String diskName, String dstIp) {
        if (diskName == null) {
            return false;
        }
        List<String> diskList = new ArrayList<>();
        if (diskName.contains("@")) {
            Collections.addAll(diskList, diskName.split("#"));
        } else {
            diskList.add(getDiskName(dstIp, diskName));
        }
        for (String s : diskList) {
            if (!RemovedDisk.getInstance().contains(s)) {
                return true;
            }
        }
        return false;
    }

    private static void basicPublish(ERROR_MQ_TYPE type, String dstIp, SocketReqMsg msg, ErrorConstant.ECErrorType errorType) {
        try {
            msg.setMsgType(errorType.name());
            String poolQueueTag = msg.getDataMap().get("poolQueueTag");//标识此消息应发往哪个队列
            Channel channel = initChannelByIp("127.0.0.1", type);
            String routingKey;
            if (StringUtils.isNotEmpty(poolQueueTag)) {//poolQueueTag不为空时发到存储池队列
                routingKey = type.key() + "_" + poolQueueTag + getErrorMqSuffix(dstIp);
            } else {//对于PREPARE_ADD_NODE中只加节点不进行存储池扩容的情况，由于不存在存储池，因此发送到公共EC队列中
                routingKey = type.key() + getErrorMqSuffix(dstIp);
            }
//            String routingKey = type.key() + getErrorMqSuffix(dstIp);
            channel.basicPublish(OBJ_EXCHANGE, routingKey, MessageProperties.PERSISTENT_TEXT_PLAIN, Json.encode(msg).getBytes());
        } catch (Exception e) {
            log.error("", e);
        }
    }

    /**
     * 只发往一般队列，不进行磁盘状态判断或计数。
     *
     * @注意 与对象或者存储池相关的修复消息请在消息内容中携带poolQueueTag信息(value为相关的存储池名称)用于队列的分发，不携带则不会分发至存储池相关队列
     */
    public static void basicPublish(String dstIp, SocketReqMsg msg, ErrorConstant.ECErrorType errorType) {
        basicPublish(EC_ERROR, dstIp, msg, errorType);
    }

    public static void basicPublishToLowSpeed(ErrorConstant.ECErrorType errorType, SocketReqMsg errorMsg) {
        errorMsg.setMsgType(errorType.name());
        Channel channel = initChannelByIp("127.0.0.1", LOW_SPEED);
        String routingKey = LOW_SPEED.key() + getErrorMqSuffix(CURRENT_IP);
        try {
            channel.basicPublish(OBJ_EXCHANGE, routingKey, MessageProperties.PERSISTENT_TEXT_PLAIN, Json.encode(errorMsg).getBytes());
        } catch (Exception e) {
            log.error("publish to low speed queue error, ", e);
        }
    }
}
