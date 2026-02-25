package com.macrosan.filesystem.cifs.api.smb2;

import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.cifs.SMB2;
import com.macrosan.filesystem.cifs.SMB2.SMB2Reply;
import com.macrosan.filesystem.cifs.SMB2Header;
import com.macrosan.filesystem.cifs.call.smb2.IOCTLCall;
import com.macrosan.filesystem.cifs.call.smb2.NegprotCall;
import com.macrosan.filesystem.cifs.reply.smb2.IOCTLReply;
import com.macrosan.filesystem.cifs.types.NegprotInfo;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.filesystem.cifs.types.smb2.IOCTLSubCall;
import com.macrosan.filesystem.cifs.types.smb2.IOCTLSubReply;
import com.macrosan.filesystem.cifs.types.smb2.IOCTLSubReply.NetworkInterfaceInfo;
import com.macrosan.filesystem.cifs.types.smb2.IOCTLSubReply.NetworkInterfaceInfo.IPV4;
import com.macrosan.filesystem.cifs.types.smb2.IOCTLSubReply.NetworkInterfaceInfo.Interface;
import com.macrosan.filesystem.cifs.types.smb2.IOCTLSubReply.ObjectId1;
import com.macrosan.filesystem.cifs.types.smb2.IOCTLSubReply.ValidateNegotiateInfo;
import com.macrosan.filesystem.cifs.types.smb2.SMB2FileId;
import com.macrosan.utils.cache.ClassUtils;
import com.macrosan.utils.functional.Function3;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.regex.Pattern;
import io.netty.util.collection.IntObjectHashMap;
import lombok.extern.log4j.Log4j2;
import oshi.SystemInfo;
import oshi.hardware.NetworkIF;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static com.macrosan.filesystem.cifs.SMB2.SMB2_OPCODE.SMB2_IOCTL;
import static com.macrosan.filesystem.cifs.call.smb2.IOCTLCall.*;
import static com.macrosan.filesystem.cifs.call.smb2.NegprotCall.SMB2_NEGOTIATE_SIGNING_ENABLED;

@Log4j2
public class IOCTL {
    private static final IOCTL instance = new IOCTL();

    private static final IntObjectHashMap<Function3<SMB2Header, Session, IOCTLCall, Mono<? extends IOCTLSubReply>>> subFunctionMap = new IntObjectHashMap<>();

    private IOCTL() {
    }

    static {
        for (Method method : IOCTL.class.getDeclaredMethods()) {
            if (!Modifier.isStatic(method.getModifiers())
                    && method.getAnnotation(IOCTLSubCall.Smb2IOCTL.class) != null) {
                IOCTLSubCall.Smb2IOCTL ioctl = method.getAnnotation(IOCTLSubCall.Smb2IOCTL.class);
                Function3<SMB2Header, Session, IOCTLCall, Mono<? extends IOCTLSubReply>> function = ClassUtils.generateFunction3(method.getDeclaringClass(), method);
                subFunctionMap.put(ioctl.value(), function);
            }
        }
    }

    public static IOCTL getInstance() {
        return instance;
    }

    @SMB2.Smb2Opt(value = SMB2_IOCTL, allowIPCSession = true)
    public Mono<SMB2Reply> ioctl(SMB2Header header, Session session, IOCTLCall call) {
        SMB2Reply reply = new SMB2Reply(header);
        IOCTLReply body = new IOCTLReply();

        body.setCtlCode(call.getCtlCode());
        body.setFileId(call.getFileId());
        body.setFlags(0);


        Function3<SMB2Header, Session, IOCTLCall, Mono<? extends IOCTLSubReply>> function = subFunctionMap.get(call.getCtlCode());
        if (function == null) {
            reply.getHeader().setStatus(FsConstants.NTStatus.STATUS_NOT_IMPLEMENTED);
            return Mono.just(reply);
        }

        return function.apply(header, session, call)
                .map(subReply -> {
                    body.setSubReply(subReply);
                    reply.setBody(body);
                    return reply;
                })
                .onErrorResume(e -> {
                    if (e instanceof MsException) {
                        reply.getHeader().setStatus(((MsException) e).getErrCode());
                        return Mono.just(reply);
                    } else {
                        return Mono.error(e);
                    }
                });
    }

    @IOCTLSubCall.Smb2IOCTL(value = FSCTL_VALIDATE_NEGOTIATE_INFO)
    public Mono<ValidateNegotiateInfo> validateNegotiateInfo(SMB2Header header, Session session, IOCTLCall call) {
        NegprotInfo negprotInfo = header.getHandler().negprotInfo;

        if (negprotInfo.getDialect() == NegprotCall.SMB_2_0_2) {
            return Mono.error(new MsException(FsConstants.NTStatus.STATUS_FILE_CLOSED, ""));
        } else {
            ValidateNegotiateInfo subReply = new ValidateNegotiateInfo();
            subReply.setDialect(negprotInfo.getDialect());
            subReply.setSecurityMode(SMB2_NEGOTIATE_SIGNING_ENABLED);
            subReply.setCapabilities(negprotInfo.getFlags());
            return Mono.just(subReply);
        }
    }

    @IOCTLSubCall.Smb2IOCTL(value = FSCTL_CREATE_OR_GET_OBJECT_ID)
    public Mono<ObjectId1> createOrGetObjectID(SMB2Header header, Session session, IOCTLCall call) {
        ObjectId1 subReply = new ObjectId1();
        SMB2FileId fileId = call.getFileId();
        if (fileId.volatile_ == 0 && fileId.persistent == 0) {
            throw new MsException(FsConstants.NTStatus.STATUS_NOT_IMPLEMENTED, "");
        } else {
            if (fileId.volatile_ == -1 && fileId.persistent == -1) {
                //多个请求的情况，create请求之后附带 FSCTL_CREATE_OR_GET_OBJECT_ID 请求，返回create打开的文件的fileId
                if (null == header.getCompoundRequest() || null == header.getCompoundRequest().getFileId()) {
                    return Mono.error(new MsException(FsConstants.NTStatus.STATUS_FILE_CLOSED, ""));
                }

                subReply.setFileId(header.getCompoundRequest().getFileId());
            } else {
                subReply.setFileId(call.getFileId());
            }
        }

        return Mono.just(subReply);
    }

    @IOCTLSubCall.Smb2IOCTL(value = FSCTL_GET_OBJECT_ID)
    public Mono<ObjectId1> getObjectID(SMB2Header header, Session session, IOCTLCall call) {
        ObjectId1 subReply = new ObjectId1();
        subReply.setFileId(call.getFileId());
        return Mono.just(subReply);
    }

    @IOCTLSubCall.Smb2IOCTL(value = FSCTL_DFS_GET_REFERRALS)
    public Mono<ObjectId1> dfsGetRefers(SMB2Header header, Session session, IOCTLCall call) {
        return Mono.error(new MsException(FsConstants.NTStatus.STATUS_NOT_FOUND, ""));
    }

    private static final Pattern ethPattern = Pattern.compile("^eth[0-3]$");

    @IOCTLSubCall.Smb2IOCTL(value = FSCTL_QUERY_NETWORK_INTERFACE_INFO)
    public Mono<NetworkInterfaceInfo> queryInterfaceInfo(SMB2Header header, Session session, IOCTLCall call) {
        NetworkInterfaceInfo info = new NetworkInterfaceInfo();
        try {
            SystemInfo systemInfo = new SystemInfo();
            int i=0;
            for (NetworkIF networkIF : systemInfo.getHardware().getNetworkIFs()) {
                if (ethPattern.matcher(networkIF.getName()).find()) {
                    for (String ip : networkIF.getIPv4addr()) {
                        info.interfaceList.add(new Interface(++i, 1, networkIF.getSpeed() / 10, new IPV4((short) 0, IPV4.getIP(ip))));
                    }
                }
            }
        } catch (Exception e) {
            log.error("", e);
        }

        return Mono.just(info);
    }
}
