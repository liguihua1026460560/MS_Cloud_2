package com.macrosan.filesystem.cifs.types.smb2.pipe;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.filesystem.cifs.Ipc.IpcBind;
import com.macrosan.filesystem.cifs.rpc.pdu.call.RpcRequestCall;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.macrosan.constants.SysConstants.REDIS_USERINFO_INDEX;
import static com.macrosan.filesystem.FsConstants.NTStatus.STATUS_INVALID_PARAMETER;
import static com.macrosan.filesystem.FsConstants.NTStatus.STATUS_NO_SUCH_USER;
import static com.macrosan.filesystem.cifs.Ipc.IpcBind.createFaultResponse;
import static com.macrosan.filesystem.cifs.rpc.RPCConstants.*;
import static com.macrosan.filesystem.cifs.rpc.RPCConstants.rpcOpnum.*;
import static com.macrosan.filesystem.cifs.types.smb2.pipe.RpcPipeType.*;

@Log4j2
public class RpcBindGenerator {

    /**
     * 生成 BindAck 响应
     *
     * @param bindPduInfo Bind 请求解析结果
     * @param pipeType    当前管道类型
     * @return BindAck 的完整字节数组（失败返回空数组）
     */
    public static byte[] generateBindAck(BindPduInfo bindPduInfo, RpcPipeType pipeType) {
        if (bindPduInfo == null) {
            return new byte[0];
        }

        if (pipeType == null) {
            return new byte[0];
        }

        BindAckInfo ackInfo = new BindAckInfo();
        Random random = new Random();
        int assocGroup = random.nextInt(0x7FFFFFFF) + 1;

        ackInfo.setCallId(bindPduInfo.getCallId());
        ackInfo.setAssocGroup(assocGroup);

        String scndryAddr = pipeType.getScndryAddr();
        ackInfo.setScndryAddr(scndryAddr);
        byte[] addrBytes = (scndryAddr + "\0").getBytes(StandardCharsets.US_ASCII);
        ackInfo.setScndryAddrLen((short) addrBytes.length);

        List<BindAckInfo.CtxItem> ackCtxItems = new ArrayList<>();

        if (pipeType == LSARPC || pipeType == SAMR) {

            ackInfo.setNumResults((short) 1);

            BindPduInfo.ContextItem first = bindPduInfo.getContextItems().get(0);
            BindAckInfo.CtxItem ackCtx = new BindAckInfo.CtxItem();
            ackCtx.setAckResult(ACCEPTANCE);  // 接受
            ackCtx.setTransferSyntaxRawBytes(first.getTransferSyntaxRawBytes());
            ackCtx.setTransferSyntaxVersion(first.getTransferSyntaxVersion());
            ackCtxItems.add(ackCtx);
            ackInfo.setCtxItems(ackCtxItems);

        } else if (pipeType == WKSSVC){
            ackInfo.setNumResults((short) 3);

            if (bindPduInfo.getContextItems().size() >= 3) {
                // 第一个：拒绝
                BindAckInfo.CtxItem reject1 = new BindAckInfo.CtxItem();
                reject1.setAckResult(PROVIDER_REJECTION);
                reject1.setTransferSyntaxVersion(0);
                ackCtxItems.add(reject1);

                // 第二个：接受 64-bit NDR
                BindPduInfo.ContextItem second = bindPduInfo.getContextItems().get(1);
                BindAckInfo.CtxItem accept = new BindAckInfo.CtxItem();
                accept.setAckResult(ACCEPTANCE);
                accept.setTransferSyntaxRawBytes(second.getTransferSyntaxRawBytes());
                accept.setTransferSyntaxVersion(second.getTransferSyntaxVersion());
                ackCtxItems.add(accept);

                // 第三个：Negotiate ACK
                BindAckInfo.CtxItem negotiate = new BindAckInfo.CtxItem();
                negotiate.setAckResult(NEGOTIATE_ACK);
                negotiate.setTransferSyntaxVersion(0);
                ackCtxItems.add(negotiate);

                log.debug("{}: Accept second context (64-bit NDR), reject first, negotiate third", pipeType.getName());
            } else {
                log.debug("Not enough contexts for {}: {}", pipeType.getName(), bindPduInfo.getContextItems().size());
            }
            ackInfo.setCtxItems(ackCtxItems);
        }

        ByteBuf buf = Unpooled.buffer(120); // 预估大小
        ackInfo.writeStruct(buf, 0);

        byte[] bindAck = new byte[buf.writerIndex()];
        buf.readBytes(bindAck);
        buf.release();

        return bindAck;
    }

    /**
     * 生成 Alter_context_resp (pduType=0x0F)
     * - 使用 alterPdu 中的 callId、frag 参数等
     * - 只返回本次 Alter_context 的上下文结果
     */
    public static byte[] generateAlterContextResp(BindPduInfo alterPdu, RpcPipeType pipeType) {

        if (alterPdu == null) {
            return new byte[0];
        }

        if (pipeType == null) {
            return new byte[0];
        }

        BindAckInfo respInfo = new BindAckInfo();
        Random random = new Random();
        int assocGroup = random.nextInt(0x7FFFFFFF) + 1;

        respInfo.setPduType(ALTER_CONTEXT_RESP);  // Alter_context_resp
        respInfo.setCallId(alterPdu.getCallId());
        respInfo.setAssocGroup(assocGroup);

        String scndryAddr = pipeType.getScndryAddr();
        respInfo.setScndryAddr(scndryAddr);
        byte[] addrBytes = (scndryAddr + "\0").getBytes(StandardCharsets.US_ASCII);
        respInfo.setScndryAddrLen((short) addrBytes.length);

        respInfo.setNumResults(alterPdu.getNumCtxItems());

        List<BindAckInfo.CtxItem> respCtxItems = new ArrayList<>();
        for (BindPduInfo.ContextItem reqCtx : alterPdu.getContextItems()) {
            BindAckInfo.CtxItem respCtx = new BindAckInfo.CtxItem();
            respCtx.setAckResult((short) 0);
            respCtx.setTransferSyntaxRawBytes(reqCtx.getTransferSyntaxRawBytes());
            respCtx.setTransferSyntaxVersion(reqCtx.getTransferSyntaxVersion());
            respCtxItems.add(respCtx);
        }
        respInfo.setCtxItems(respCtxItems);

        // 序列化
        ByteBuf buf = Unpooled.buffer(120);
        int length = respInfo.writeStruct(buf, 0);

        byte[] resp = new byte[buf.writerIndex()];
        buf.readBytes(resp);
        buf.release();

        return resp;
    }


    public static byte[] generateRequestPdu(RpcRequestCall rpcRequestCall, SMB2FileId.FileInfo fileInfo) {
        RedisConnPool redisConnPool = RedisConnPool.getInstance();
        byte[] stubData = rpcRequestCall.stubData;
        byte[] responsePDU = new byte[0];

        switch (rpcRequestCall.header.opnum) {
            case DsRoleGetPrimary_OR_NetWkstaGetInfo: {
                if ("lsarpc".equals(fileInfo.obj)) {
                    int infoLevel = 0;
                    if (stubData.length == 2) {
                        infoLevel = ByteBuffer.wrap(stubData, 0, 2).order(ByteOrder.LITTLE_ENDIAN).getShort() & 0xFFFF;
                        responsePDU = IpcBind.createDsRoleResponsePDU(
                                rpcRequestCall.header.callId,
                                rpcRequestCall.header.contextId,
                                infoLevel
                        );
                    }else if (stubData.length == 20){
                        responsePDU = IpcBind.lsaCloseResponsePDU(
                                rpcRequestCall.header.callId,
                                rpcRequestCall.header.contextId
                        );
                    }

                } else if ("wkssvc".equals(fileInfo.obj)) {
                    int level = 0;
                    ByteBuffer bb = ByteBuffer.wrap(stubData).order(ByteOrder.LITTLE_ENDIAN);
                    if (stubData.length >= 8) {
                        int serverNamePtr = bb.getInt(0);

                        if (serverNamePtr == 0) {

                            if (stubData.length >= 8) {
                                level = bb.getInt(4);
                            }
                        } else {
                            level = bb.getInt(4);
                        }
                    }

                    responsePDU = IpcBind.createWkstaGetInfoResponse(
                            rpcRequestCall.header.callId,
                            rpcRequestCall.header.contextId,
                            level
                    );
                }
                break;
            }

            case closePipe: {
                if ("samr".equals(fileInfo.obj)) {
                    responsePDU = IpcBind.closeResponsePDU(
                            rpcRequestCall.header.callId,
                            rpcRequestCall.header.contextId
                    );
                }
                break;
            }

            case querySecurity: {
                if ("samr".equals(fileInfo.obj)) {
                    responsePDU = IpcBind.querySecurityResponsePDU(
                            rpcRequestCall.header.callId,
                            rpcRequestCall.header.contextId,
                            fileInfo.uidAndGid,
                            fileInfo.isGroupOrUser
                    );
                }
                break;
            }

            case lookupDomain: {
                if ("samr".equals(fileInfo.obj)) {
                    responsePDU = IpcBind.lookupDomainResponsePDU(
                            rpcRequestCall.header.callId,
                            rpcRequestCall.header.contextId
                    );
                }
                break;
            }

            case enumDomains: {
                if ("samr".equals(fileInfo.obj)) {
                    responsePDU = IpcBind.enumDomainsResponsePDU(
                            rpcRequestCall.header.callId,
                            rpcRequestCall.header.contextId
                    );
                }
                break;
            }

            case createLsaQuery_OR_openDomain: {
                if ("lsarpc".equals(fileInfo.obj)) {
                    int level = 0;
                    if (stubData.length >= 22) {
                        level = ByteBuffer.wrap(stubData, stubData.length - 2, 2).order(ByteOrder.LITTLE_ENDIAN).getShort();
                    }
                    responsePDU = IpcBind.createLsaQueryInfoPolicyResponse(
                            rpcRequestCall.header.callId,
                            rpcRequestCall.header.contextId,
                            level
                    );
                } else if ("samr".equals(fileInfo.obj)) {
                    responsePDU = IpcBind.openDomainResponsePDU(
                            rpcRequestCall.header.callId,
                            rpcRequestCall.header.contextId
                    );
                }
                break;
            }


            case lookupNames: {
                if ("samr".equals(fileInfo.obj)) {
                    String accountName = IpcBind.parseSamrLookupName(stubData);
                    if (accountName == null) {
                        log.debug("Invalid account name");
                        responsePDU = createFaultResponse(rpcRequestCall.header.callId, rpcRequestCall.header.contextId, STATUS_NO_SUCH_USER);
                        break;
                    }

                    String targetName;

                    boolean isGroup = accountName.endsWith("_xgrp");
                    boolean isUser = accountName.endsWith("_xusr");

                    if (isGroup || isUser){
                        fileInfo.targetName = accountName;
                        targetName = accountName.substring(0, accountName.length() - 5);

                        if (isGroup) {
                            fileInfo.isGroupOrUser = 0;
                        } else if (isUser) {
                            fileInfo.isGroupOrUser = 1;
                        }

                        String s3Id = redisConnPool.getCommand(REDIS_USERINFO_INDEX).hget(targetName,"id");
                        if (StringUtils.isEmpty(s3Id)) {
                            responsePDU = createFaultResponse(rpcRequestCall.header.callId,
                                    rpcRequestCall.header.contextId,
                                    STATUS_NO_SUCH_USER);
                            break;
                        }
                        int[] uidAndGid = ACLUtils.getUidAndGid(s3Id);
                        if (uidAndGid == null || uidAndGid.length != 2 || uidAndGid[0] <= 0) {
                            log.debug("uidAndGid 无效或不存在: targetName={}, s3Id={}, uidAndGid={}",
                                    accountName, s3Id, Arrays.toString(uidAndGid));
                            responsePDU = createFaultResponse(rpcRequestCall.header.callId,
                                    rpcRequestCall.header.contextId,
                                    STATUS_NO_SUCH_USER);
                            break;
                        }
                        fileInfo.uidAndGid = uidAndGid;
                    } else {
                        fileInfo.targetName = accountName;
                        String s3Id = redisConnPool.getCommand(REDIS_USERINFO_INDEX).hget(accountName,"id");
                        if (StringUtils.isEmpty(s3Id)) {
                            responsePDU = createFaultResponse(rpcRequestCall.header.callId,
                                    rpcRequestCall.header.contextId,
                                    STATUS_NO_SUCH_USER);
                            break;
                        }
                        int[] uidAndGid = ACLUtils.getUidAndGid(s3Id);
                        if (uidAndGid == null || uidAndGid.length != 2 || uidAndGid[0] <= 0) {
                            log.debug("uidAndGid 无效或不存在: targetName={}, s3Id={}, uidAndGid={}",
                                    accountName, s3Id, Arrays.toString(uidAndGid));
                            responsePDU = createFaultResponse(rpcRequestCall.header.callId,
                                    rpcRequestCall.header.contextId,
                                    STATUS_NO_SUCH_USER);
                            break;
                        }
                        fileInfo.uidAndGid = uidAndGid;
                    }
                    int rid = 1000 + Math.abs(accountName.hashCode() % 9000);
                    responsePDU = IpcBind.lookupNamesResponsePDU(
                            rpcRequestCall.header.callId,
                            rpcRequestCall.header.contextId,
                            rid
                    );
                }
                break;
            }

            case openUser: {
                if ("samr".equals(fileInfo.obj)) {
                    responsePDU = IpcBind.openUserResponsePDU(
                            rpcRequestCall.header.callId,
                            rpcRequestCall.header.contextId
                    );
                }
                break;
            }

            case queryUserInfo: {
                if ("samr".equals(fileInfo.obj)) {
                    responsePDU = IpcBind.queryUserInfoResponsePDU(
                            rpcRequestCall.header.callId,
                            rpcRequestCall.header.contextId,
                            fileInfo.targetName,
                            fileInfo.uidAndGid
                    );
                }
                break;
            }

            case getGroupForUser: {
                if ("samr".equals(fileInfo.obj)) {
                    responsePDU = IpcBind.getGroupForUserResponsePDU(
                            rpcRequestCall.header.callId,
                            rpcRequestCall.header.contextId
                    );
                }
                break;
            }

            case LsaOpenPolicy2:
            {
                if ("lsarpc".equals(fileInfo.obj)) {
                    responsePDU = IpcBind.LsaOpenPolicy2Response(
                            rpcRequestCall.header.callId,
                            rpcRequestCall.header.contextId
                    );
                }
                break;
            }


            case lsaLookupSids2:
            {
                if (stubData.length < 76) {
                    log.debug("LsaLookupSids2 stub too short: {}", stubData.length);
                    responsePDU = createFaultResponse(
                            rpcRequestCall.header.callId,
                            rpcRequestCall.header.contextId,
                            STATUS_INVALID_PARAMETER);
                }
                else {
                    responsePDU = IpcBind.lsaLookupSids2Response(
                            rpcRequestCall.header.callId,
                            rpcRequestCall.header.contextId
                    );
                }
                break;
            }

            case connect5:
            {
                if ("samr".equals(fileInfo.obj)) {
                    responsePDU = IpcBind.connect5ResponsePDU(
                            rpcRequestCall.header.callId,
                            rpcRequestCall.header.contextId
                    );
                }
                break;
            }

            case lsaLookupNames3:
            {
                if ("lsarpc".equals(fileInfo.obj)) {
                    String accountName = IpcBind.parseLsaLookupNames3Name(stubData);

                    boolean isGroup = accountName.endsWith("_xgrp");
                    boolean isUser = accountName.endsWith("_xusr");
                    if (isGroup){
                        accountName = accountName.substring(0, accountName.length() - 5);
                        fileInfo.isGroupOrUser = 0;
                    } else if (isUser) {
                        accountName = accountName.substring(0, accountName.length() - 5);
                        fileInfo.isGroupOrUser = 1;
                    }
                    String s3Id = redisConnPool.getCommand(REDIS_USERINFO_INDEX).hget(accountName,"id");
                    int[] uidAndGid = ACLUtils.getUidAndGid(s3Id);

                    responsePDU = IpcBind.lsaLookupNames3ResponsePDU(
                            rpcRequestCall.header.callId,
                            rpcRequestCall.header.contextId,
                            fileInfo.isGroupOrUser,
                            uidAndGid
                    );
                }
                break;
            }

            case LsaOpenPolicy3:
            {
                if ("lsarpc".equals(fileInfo.obj)) {
                    responsePDU = IpcBind.LsaOpenPolicy3Response(
                            rpcRequestCall.header.callId,
                            rpcRequestCall.header.contextId
                    );
                }
                break;
            }
        }
        return responsePDU;
    }
}