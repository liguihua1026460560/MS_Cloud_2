package com.macrosan.action.managestream;

import com.alibaba.fastjson.JSON;
import com.macrosan.action.core.BaseService;
import com.macrosan.component.param.ImageWatermarkImageParams;
import com.macrosan.component.param.ProcessParams;
import com.macrosan.component.pojo.ComponentRecord;
import com.macrosan.component.utils.ParamsUtils;
import com.macrosan.constants.ErrorNo;
import com.macrosan.filesystem.utils.CheckUtils;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.xmlmsg.ComponentStrategy;
import com.macrosan.message.xmlmsg.ComponentTask;
import com.macrosan.message.xmlmsg.ListComponentStrategyResponse;
import com.macrosan.message.xmlmsg.ListComponentTaskResponse;
import com.macrosan.snapshot.utils.SnapshotUtil;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.serialize.JaxbUtils;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanIterator;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.*;
import java.util.stream.Collectors;

import static com.macrosan.component.ComponentStarter.DICOM_SUPPORT_DESTINATION;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;

/**
 * @author zhaoyang
 * @date 2023/07/24
 **/
@Log4j2
public class ComponentStrategyService extends BaseService {

    private ComponentStrategyService() {
        super();
    }

    private static ComponentStrategyService instance = null;

    public static ComponentStrategyService getInstance() {
        if (instance == null) {
            instance = new ComponentStrategyService();
        }
        return instance;
    }

    /**
     * 新增策略
     *
     * @param paramMap
     * @return
     */
    public ResponseMsg putComponentStrategy(UnifiedMap<String, String> paramMap) {
        //鉴权
        String userId = paramMap.get(USER_ID);
        MsAclUtils.checkIfManageAccount(userId);

        String name = paramMap.get(COMPONENT_STRATEGY_NAME);
        String destination = "null".equals(paramMap.get(COMPONENT_STRATEGY_DESTINATION)) ? "" : paramMap.get(COMPONENT_STRATEGY_DESTINATION);
        boolean deleteSource = Boolean.parseBoolean(paramMap.get(COMPONENT_STRATEGY_DELETE_SOURCE));
        boolean copyUserMetaData = StringUtils.isBlank(paramMap.get(COMPONENT_STRATEGY_COPY_USER_META_DATA)) ||
                Boolean.parseBoolean(paramMap.get(COMPONENT_STRATEGY_COPY_USER_META_DATA));
        String process = null;
        String processType = null;
        for (ComponentRecord.Type type : ComponentRecord.Type.values()) {
            process = paramMap.get(type.name);
            if (StringUtils.isNotBlank(process)) {
                processType = type.name;
                break;
            }
        }
        //校验name和processs是否为空
        if (StringUtils.isEmpty(name) || StringUtils.isEmpty(process)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
        }
        //检验process参数格式
        String[] processArray = process.split("/");
        ImageWatermarkImageParams watermarkImageParams = null;
        for (String processItem : processArray) {
            ProcessParams params = ParamsUtils.getParams(processType, processItem);
            if (params instanceof ImageWatermarkImageParams) {
                watermarkImageParams = (ImageWatermarkImageParams) params;
            }
            params.checkParams();
        }
        // 校验图片水印的logg是否存在
        if (watermarkImageParams != null) {
            Boolean exists = watermarkImageParams.checkImageIsExists().block();
            if (exists == null) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "check watermark image fail");
            }
            if (!exists) {
                throw new MsException(ErrorNo.WATER_MARKER_IMAGE_NOT_FOUND, " watermark image is not found");
            }
        }
        if (ComponentRecord.Type.DICOM.name.equals(processType) && !DICOM_SUPPORT_DESTINATION) {
            // 影像压缩策略，不支持删源和目标位置
            if (StringUtils.isNotEmpty(destination) || deleteSource) {
                throw new MsException(ErrorNo.NOT_SUPPORTED_COMPONENT_PARAM, "not supported component param");
            }
        } else {
            // 其他策略支持删源和目标位置，需要进行参数校验
            //判断destination是否为/开头或结尾
            if (!StringUtils.isEmpty(destination) && (destination.startsWith("/") || destination.endsWith("/"))) {
                throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
            }
            // 判断和删源是覆盖否冲突
            if (ComponentRecord.Type.IMAGE.name().equals(processType) && StringUtils.isBlank(destination) && deleteSource) {
                throw new MsException(ErrorNo.DELETE_SOURCE_CONFLICT, "destination is empty, delete source conflict");
            }
        }

        ScanIterator<String> iterator = ScanIterator.scan(pool.getCommand(REDIS_POOL_INDEX), new ScanArgs().match(COMPONENT_STRATEGY_REDIS_PREFIX + "*"));
        while (iterator.hasNext()) {
            String key = iterator.next();
            Map<String, String> strategyMap = pool.getCommand(REDIS_POOL_INDEX).hgetall(key);
            String redisStrategyName = strategyMap.get("strategyName");
            String redisDestination = strategyMap.get("destination");
            //判断策略名称是否已存在
            if (Objects.equals(name, redisStrategyName)) {
                throw new MsException(ErrorNo.STRATEGY_NAME_ALREADY_EXISTS, "strategy name already exists,strategy:" + name);
            }
            // 目标位置不为空，才需要进行判断
            if (!StringUtils.isEmpty(destination) && !StringUtils.isEmpty(redisDestination)) {
                String tempDestination = destination + "/";
                String tempRedisDestination = redisDestination + "/";
                // 是否重复
                if (Objects.equals(tempDestination, tempRedisDestination)) {
                    throw new MsException(ErrorNo.STRATEGY_DESTINATION_ALREADY_EXISTS, "The destination already used by other strategies,destination:" + destination);
                }
                //  判断两个策略是否有包含、被包含关系
                if (tempDestination.startsWith(tempRedisDestination) || tempRedisDestination.startsWith(tempDestination)) {
                    throw new MsException(ErrorNo.STRATEGY_DESTINATION_CONTAINS, "The destination has an contains relationship with other strategies,destination:" + destination);
                }
            }
        }
        //判断目标桶是否存在
        if (!StringUtils.isEmpty(destination)) {
            String targetBucket = destination.split("/")[0];
            Long exists = pool.getCommand(REDIS_BUCKETINFO_INDEX).exists(targetBucket);
            if (exists != 1L) {
                throw new MsException(ErrorNo.TARGET_BUCKET_NOT_EXISTS, "The target bucket does not exists,targetBucket:" + targetBucket);
            }
            if (CheckUtils.bucketFsCheck(targetBucket)) {
                throw new MsException(ErrorNo.NFS_NOT_STOP, "The bucket already start nfs or cifs, can not add componentStrategy");
            }
            // 判断目标桶是否为本区域的桶
            String regionName = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(targetBucket, REGION_NAME);
            // 目标桶是否开启桶快照
            SnapshotUtil.checkOperationCompatibility(SnapshotUtil.checkBucketSnapshotEnable(targetBucket) ? "on" : null);
            regionCheck(regionName);
        }
        //封装map，存入redis中
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put("strategyName", name);
        valueMap.put("process", process);
        valueMap.put("type", processType);
        valueMap.put("destination", destination);
        valueMap.put("strategyMark", RandomStringUtils.randomAlphanumeric(10).toLowerCase());
        valueMap.put("deleteSource", String.valueOf(deleteSource));
        valueMap.put("copyUserMetaData", String.valueOf(copyUserMetaData));
        pool.getShortMasterCommand(REDIS_POOL_INDEX).hmset(COMPONENT_STRATEGY_REDIS_PREFIX + name, valueMap);
        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    /**
     * 删除指定名称的策略
     *
     * @param paramMap
     * @return
     */
    public ResponseMsg deleteComponentStrategy(UnifiedMap<String, String> paramMap) {
        //鉴权
        String userId = paramMap.get(USER_ID);
        MsAclUtils.checkIfManageAccount(userId);
        String name = paramMap.get(COMPONENT_STRATEGY_NAME);
        if (StringUtils.isEmpty(name)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
        }
        //判断是否有task使用该策略，如果有则不能删除该策略
        //获取所有的task
        ComponentTaskService componentTaskService = new ComponentTaskService();
        ListComponentTaskResponse listComponentTaskResponse = componentTaskService.getListComponentTaskResponse();
        List<ComponentTask> componentTaskList = listComponentTaskResponse.getComponentTaskList();
        //判断当前策略是否正在被某个task使用
        if (componentTaskList != null && !componentTaskList.isEmpty()) {
            long count = componentTaskList.stream()
                    .filter(componentTask -> componentTask.getStrategyName().equals(name))
                    .count();
            if (count > 0) {
                throw new MsException(ErrorNo.DELETE_COMPONENT_STRATEGY_FAIL, "this strategy is being used by some tasks and cannot be deleted");
            }
        }
        //删除指定name的策略
        Long delCode = pool.getShortMasterCommand(REDIS_POOL_INDEX).del(COMPONENT_STRATEGY_REDIS_PREFIX + name);
        if (-1 == delCode) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "delete component strategy fail; name is " + name);
        }
        return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
    }

    /**
     * 获取策略列表
     *
     * @param paramMap
     * @return
     */
    public ResponseMsg listComponentStrategies(UnifiedMap<String, String> paramMap) {
        //鉴权
        String userId = paramMap.get(USER_ID);
        String strategyType = paramMap.get("strategyType");
        MsAclUtils.checkIfManageAccount(userId);
        //扫描获取所有componentStrategy的key
        List<String> keys = new ArrayList<>();
        ScanIterator<String> iterator = ScanIterator.scan(pool.getCommand(REDIS_POOL_INDEX), new ScanArgs().match(COMPONENT_STRATEGY_REDIS_PREFIX + "*"));
        while (iterator.hasNext()) {
            String key = iterator.next();
            if (!StringUtils.isEmpty(key)) {
                keys.add(key);
            }
        }
        //遍历key，依次查询对应的数据，并将其转成对应的实体类
        ListComponentStrategyResponse listComponentStrategyResponse = new ListComponentStrategyResponse();
        if (!keys.isEmpty()) {
            List<ComponentStrategy> componentStrategyList = keys.stream()
                    .map(key -> pool.getCommand(REDIS_POOL_INDEX).hgetall(key))
                    .map(map -> JSON.parseObject(JSON.toJSONString(map), ComponentStrategy.class))
                    .filter(componentStrategy -> StringUtils.isEmpty(strategyType) || StringUtils.equals(componentStrategy.getType(), strategyType))
                    .collect(Collectors.toList());
            listComponentStrategyResponse = new ListComponentStrategyResponse()
                    .setComponentStrategyList(componentStrategyList);
        }
        //序列化为xml
        byte[] data = JaxbUtils.toByteArray(listComponentStrategyResponse);
        return new ResponseMsg().setData(data).addHeader(CONTENT_TYPE, XML_RESPONSE);
    }

    /**
     * 获取指定name的策略
     *
     * @param paramMap
     * @return
     */
    public ResponseMsg getComponentStrategy(UnifiedMap<String, String> paramMap) {
        //鉴权
        String userId = paramMap.get(USER_ID);
        MsAclUtils.checkIfManageAccount(userId);
        String name = paramMap.get(COMPONENT_STRATEGY_NAME);
        //判断name是否为空
        if (StringUtils.isEmpty(name)) {
            throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
        }
        Map<String, String> componentStrategyMap = pool.getCommand(REDIS_POOL_INDEX).hgetall(COMPONENT_STRATEGY_REDIS_PREFIX + name);
        if (componentStrategyMap == null || componentStrategyMap.isEmpty()) {
            throw new MsException(ErrorNo.NO_SUCH_COMPONENT_STRATEGY, "no such component strategy;  name:" + name);
        }
        //将redis中查出来的map转成对应实体类
        ComponentStrategy componentStrategy = JSON.parseObject(JSON.toJSONString(componentStrategyMap), ComponentStrategy.class);
        //序列化为xml格式
        ListComponentStrategyResponse listComponentStrategyResponse = new ListComponentStrategyResponse()
                .setComponentStrategyList(Collections.singletonList(componentStrategy));
        byte[] data = JaxbUtils.toByteArray(listComponentStrategyResponse);
        return new ResponseMsg().setData(data).addHeader(CONTENT_TYPE, XML_RESPONSE);
    }


    /**
     * 检验process中的每一个参数项
     *
     * @param processItems 参数项
     */
    private void checkParams(String[] processItems) {
        for (String processItem : processItems) {
            //判断缩放比例scale的值是否为正小数或者0
            if (processItem.startsWith("scale_")) {
                String value = processItem.split("_")[1];
                boolean matches = value.matches("^0|\\d+|\\d+\\.\\d+$");
                if (!matches) {
                    throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
                }
                continue;
            }
            //判断x、y、size的值是否为正整数
            if (processItem.startsWith("x_") || processItem.startsWith("y_") || processItem.startsWith("size_")
                    || processItem.startsWith("h_") || processItem.startsWith("w_")) {
                String value = processItem.split("_")[1];
                boolean matches = value.matches("^0|[1-9]\\d*$");
                if (!matches) {
                    throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
                }
                continue;
            }
            //判断color数值格式
            if (processItem.startsWith("color_")) {
                String color = processItem.split("_")[1];
                boolean matches = color.matches("^#[A-Fa-f0-9]{6}$");
                if (!matches) {
                    throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
                }
                continue;
            }
            //判断水印内容不能为空
            if (processItem.startsWith("word_")) {
                String[] wordArray = processItem.split("_");
                if (wordArray.length < 2 || StringUtils.isEmpty(wordArray[1])) {
                    throw new MsException(ErrorNo.INVALID_COMPONENT_PARAM, "invalid component param");
                }
                //水印内容不为空，判断是否为base64编码
                try {
                    Base64.getUrlDecoder().decode(wordArray[1]);
                } catch (Exception e) {
                    throw new MsException(ErrorNo.NOT_BASE64_ENCODING, " text is invalid.reason:Input is not base64 decoding");
                }
            }
        }
    }

}
