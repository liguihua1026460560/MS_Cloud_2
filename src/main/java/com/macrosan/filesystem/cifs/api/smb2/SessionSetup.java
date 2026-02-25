package com.macrosan.filesystem.cifs.api.smb2;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.filesystem.cifs.SMB2;
import com.macrosan.filesystem.cifs.SMB2.SMB2Reply;
import com.macrosan.filesystem.cifs.SMB2.SMB2_OPCODE;
import com.macrosan.filesystem.cifs.SMB2.Smb2Opt;
import com.macrosan.filesystem.cifs.SMB2Header;
import com.macrosan.filesystem.cifs.call.smb2.*;
import com.macrosan.filesystem.cifs.lease.LeaseCache;
import com.macrosan.filesystem.cifs.lock.CIFSLockClient;
import com.macrosan.filesystem.cifs.reply.smb2.*;
import com.macrosan.filesystem.cifs.shareAccess.ShareAccessClient;
import com.macrosan.filesystem.cifs.types.NTLMSSP;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.filesystem.cifs.types.Spnego;
import com.macrosan.filesystem.cifs.types.Spnego.SpnegoMessage;
import com.macrosan.filesystem.cifs.types.Spnego.SpnegoTokenTargMessage;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.nfs.auth.KrbJni;
import com.macrosan.filesystem.utils.CheckUtils;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.utils.msutils.MsException;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.util.Arrays;

import static com.macrosan.constants.AccountConstants.DEFAULT_USER_ID;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.FsConstants.NTStatus.*;
import static com.macrosan.filesystem.cifs.CIFS.cifsDebug;
import static com.macrosan.filesystem.cifs.SMB2.SMB2_OPCODE.SMB2_LOGOFF;
import static com.macrosan.filesystem.cifs.SMB2.SMB2_OPCODE.SMB2_TDIS;
import static com.macrosan.filesystem.cifs.reply.smb1.TreeConnectXReply.FILE_ALL_ACCESS;
import static com.macrosan.filesystem.cifs.reply.smb1.TreeConnectXReply.STANDARD_RIGHTS_ALL_ACCESS;
import static com.macrosan.filesystem.cifs.reply.smb2.TreeConnectReply.*;
import static com.macrosan.filesystem.cifs.rpc.witness.WITNESS.cifsIPRegister;
import static com.macrosan.filesystem.cifs.types.NTLMSSP.*;
import static com.macrosan.filesystem.utils.acl.CIFSACL.checkCIFSBucketMount;
import static java.nio.charset.StandardCharsets.UTF_16LE;

@Log4j2
public class SessionSetup {
    private static final SessionSetup instance = new SessionSetup();
    private static RedisConnPool pool = RedisConnPool.getInstance();

    private SessionSetup() {
    }

    public static SessionSetup getInstance() {
        return instance;
    }

    @Smb2Opt(value = SMB2_OPCODE.SMB2_SESSSETUP, allowNoSession = true)
    public Mono<SMB2Reply> ntlmsspSetup(SMB2Header header, Session session, NTLMSSPSetupXCall call) {
        // 注册连接的服务端ip地址
        if (cifsIPRegister) {
            String serverIp = session.socket.localAddress().host();
            String clientIp = session.socket.remoteAddress().host();
            RedisConnPool.getInstance().getShortMasterCommand(REDIS_SYSINFO_INDEX)
                    .hset("cifs_witness_list", clientIp, serverIp);
        }
        NTLMSSPMessage msg = call.getMessage();
        SMB2Reply reply = new SMB2Reply(header);

        if (msg instanceof SpnegoMessage) {
            SpnegoMessage spnegoMsg = (SpnegoMessage) msg;
            return msgHandler(header, reply, session, spnegoMsg.token)
                    .flatMap(reply1 -> {
                        if (reply.getBody() instanceof NTLMSSPSetupXReply) {
                            NTLMSSPSetupXReply replyBody = (NTLMSSPSetupXReply) reply.getBody();
                            SpnegoTokenTargMessage resMsg = new SpnegoTokenTargMessage();
                            if (spnegoMsg.token instanceof Spnego.SpnegoTokenInitMessage) {
                                //回退到NTLM
//                                resMsg.res = 3;
//                                resMsg.supportedMech = SpnegoTokenTargMessage.GSS_NTLM_MECHANISM;
//                                replyBody.setMessage(resMsg);
//                                reply.getHeader().setStatus(STATUS_INVALID_DEVICE_REQUEST);
                                SpnegoTokenTargMessage message = (SpnegoTokenTargMessage) replyBody.message;
                                message.res = SpnegoTokenTargMessage.SPNEGO_ACCEPT_COMPLETED;
                                message.supportedMech = SpnegoTokenTargMessage.OID_MS_KERBEROS5;
                                replyBody.setMessage(message);
                                return Mono.just(reply);
                            }
                            if (replyBody.message == null) {
                                resMsg.res = SpnegoTokenTargMessage.SPNEGO_ACCEPT_COMPLETED;
                                if (spnegoMsg instanceof SpnegoTokenTargMessage && ((SpnegoTokenTargMessage) spnegoMsg).mic != null) {
                                    //TODO 暂不校验客户端发送的mic

                                    // 匿名访问不设置mic
                                    if (replyBody.flags != 1) {
                                        if (false) {
                                            byte[] recvSignKey = CifsUtils.ntlmv2Key(session.sessionKey, "session key to client-to-server signing key magic constant");
                                            byte[] recvSealKey = CifsUtils.ntlmv2Key(session.sessionKey, "session key to client-to-server sealing key magic constant");
                                            byte[] checkMic = CifsUtils.getMIC(recvSignKey, recvSealKey);
                                            if (cifsDebug) {
                                                log.debug("check mic {}", Arrays.equals(checkMic, ((SpnegoTokenTargMessage) spnegoMsg).mic));
                                            }
                                        }

                                        byte[] sendSignKey = CifsUtils.ntlmv2Key(session.sessionKey, "session key to server-to-client signing key magic constant");
                                        byte[] sendSealKey = CifsUtils.ntlmv2Key(session.sessionKey, "session key to server-to-client sealing key magic constant");
                                        resMsg.mic = CifsUtils.getMIC(sendSignKey, sendSealKey);
                                    }
                                }


                                replyBody.setMessage(resMsg);
                            } else {
                                resMsg.supportedMech = SpnegoTokenTargMessage.GSS_NTLM_MECHANISM;
                                resMsg.token = replyBody.message;
                                resMsg.res = SpnegoTokenTargMessage.SPNEGO_ACCEPT_INCOMPLETE;
                                replyBody.setMessage(resMsg);
                            }
                        }
                        if (call.getSecurityMode() != 0 && session != null) {
                            session.signed = true;
                        }
                        return Mono.just(reply);
                    });
        } else {
            return msgHandler(header, reply, session, msg).flatMap(reply1 -> {
                if (call.getSecurityMode() != 0 && session != null) {
                    session.signed = true;
                }
                return Mono.just(reply);
            });
        }
    }

    public Mono<SMB2Reply> msgHandler(SMB2Header header, SMB2Reply reply, Session session, NTLMSSPMessage msg) {
        if (msg instanceof NTLMNegotiate) {
            //session == noSession
            if (session.sessionId == 0) {
                session = Session.getNextSession2(session.session2Map, header.getHandler());
                NTLMSSPSetupXReply body = new NTLMSSPSetupXReply();
                body.flags = 0;

                NTLMChallenge challenge = new NTLMChallenge();
                challenge.targetName = "MOSS";
                challenge.serverChallenge = session.serverChallenge;
                challenge.version = 0x0f00000000000106L;
                challenge.targetInfo.put(TargetKey.NbComputerName, "MOSS");
                challenge.targetInfo.put(TargetKey.NbDomainName, "MOSS");
                challenge.targetInfo.put(TargetKey.DnsDomainName, ".com");
                challenge.targetInfo.put(TargetKey.DnsComputerName, "moss.com");
                challenge.targetInfo.put(TargetKey.Timestamp, CifsUtils.nttime(System.currentTimeMillis()));

                challenge.flags = ((NTLMNegotiate) msg).flags;
                challenge.flags |= NTLMSSP_NEGOTIATE_UNICODE | NTLMSSP_NEGOTIATE_EXTENDED_SESSIONSECURITY |
                        NTLMSSP_TARGET_TYPE_SERVER | NTLMSSP_NEGOTIATE_TARGET_INFO;
                challenge.flags &= ~NTLMSSP_NEGOTIATE_OEM & ~NTLMSSP_NEGOTIATE_LM_KEY;

                if ((((NTLMNegotiate) msg).flags & NTLMSSP_NEGOTIATE_UNICODE) != 0) {
                    challenge.flags |= NTLMSSP_NEGOTIATE_UNICODE;
                }

                body.message = challenge;
                reply.getHeader().sessionId = session.sessionId;
                reply.getHeader().status = STATUS_INVALID_DEVICE_REQUEST;
                reply.setBody(body);
                return Mono.just(reply);
            }
        } else if (msg instanceof NTLMAuth) {
            if (session.sessionId == 0) {
                log.error("no  session {}", header);
                reply.getHeader().status = STATUS_BAD_LOGON_SESSION_STATE;
                return Mono.just(reply);
            }

            NTLMSSP.NTLMAuth auth = (NTLMSSP.NTLMAuth) msg;
            if ((auth.flags & NTLMSSP_NEGOTIATE_UNICODE) != 0) {
                session.utf16 = true;
                session.account = new String(auth.userName, UTF_16LE);
                session.domain = new String(auth.domain, UTF_16LE);
            } else {
                session.utf16 = false;
                session.account = new String(auth.userName);
                session.domain = new String(auth.domain);
            }
            Session[] finalSession = new Session[]{session};
            return pool.getReactive(REDIS_USERINFO_INDEX).exists(finalSession[0].account).flatMap(exist -> {
                if (exist <= 0) {
                    // 用户不存在转匿名用户
                    NTLMSSPSetupXReply body = new NTLMSSPSetupXReply();
                    body.flags = 1; // 设置guest
                    body.message = null;
                    reply.getHeader().status = 0;
                    reply.setBody(body);
                    byte[] zeroArray = new byte[16];
                    finalSession[0].flags |= 0b1;
                    finalSession[0].sessionKey = zeroArray;
                    finalSession[0].channelSessionKey = zeroArray;
                    return Mono.just(reply);
                }

                String tempAccount = null;
                //todo: SMBCACLES 指令调用设置权限，域名默认采用"MOSS"
                if (finalSession[0].account != null && finalSession[0].account.startsWith("MOSS\\")) {
                    int index = finalSession[0].account.lastIndexOf('\\');
                    tempAccount = finalSession[0].account.substring(index + 1);
                } else {
                    tempAccount = finalSession[0].account;
                }

                String finalTempAccount = tempAccount;

                return pool.getReactive(REDIS_USERINFO_INDEX).hget(finalTempAccount, USER_DATABASE_NAME_PASSWD)
                        .flatMap(passwd -> {
                            boolean isNotPass = !auth.checkAuth(finalSession[0].domain, finalSession[0].account, passwd, finalSession[0].serverChallenge);
                            if (isNotPass) {
                                // 密码错误转匿名用户
                                NTLMSSPSetupXReply body = new NTLMSSPSetupXReply();
                                body.flags = 1; // 设置guest
                                body.message = null;
                                reply.getHeader().status = 0;
                                reply.setBody(body);
                                byte[] zeroArray = new byte[16];
                                finalSession[0].flags |= 0b1;
                                finalSession[0].sessionKey = zeroArray;
                                finalSession[0].channelSessionKey = zeroArray;
                                return Mono.just(reply);
                            }
                            byte[] responseKeyNT = CifsUtils.nTOWFv2(finalSession[0].domain, finalSession[0].account, passwd);
                            CifsUtils.HMACT64 hmac = new CifsUtils.HMACT64(responseKeyNT);
                            hmac.update(Arrays.copyOf(auth.ntlmResponse, 16));
                            byte[] userSessionKey = hmac.digest();

                            if ((auth.flags & NTLMSSP_NEGOTIATE_KEY_EXCH) != 0) {
                                finalSession[0].sessionKey = CifsUtils.rc4Encrypt(userSessionKey, auth.seesionKey);
                            } else {
                                finalSession[0].sessionKey = userSessionKey;
                            }

                            short dialect = header.getHandler().negprotInfo.getDialect();

                            if (dialect >= NegprotCall.SMB_3_1_1) {
                                return Mono.error(new MsException(STATUS_NOT_IMPLEMENTED, ""));
                            } else if (dialect >= NegprotCall.SMB_3_0_0) {
                                finalSession[0].channelSessionKey = CifsUtils.hmacSHA256(finalSession[0].sessionKey,
                                        new byte[]{0, 0, 0, 1}, "SMB2AESCMAC".getBytes(),
                                        new byte[]{0, 0}, "SmbSign".getBytes(), new byte[]{0, 0, 0, 0, -128});
                            } else {
                                finalSession[0].channelSessionKey = finalSession[0].sessionKey;
                            }

                            return pool.getReactive(REDIS_USERINFO_INDEX).hget(finalTempAccount, USER_DATABASE_NAME_ID)
                                    .map(s3Id -> {
                                        NTLMSSPSetupXReply body = new NTLMSSPSetupXReply();
                                        body.flags = 0;
                                        body.message = null;
                                        reply.getHeader().status = 0;
                                        reply.setBody(body);
                                        return reply;
                                    });
                        });
            });
        } else if (msg instanceof Spnego.SpnegoTokenInitMessage) {
            Spnego.SpnegoTokenInitMessage initMessage = (Spnego.SpnegoTokenInitMessage) msg;
            if (initMessage.krbData.length > 0) {
                try {
                    KrbJni.SmbKrbContext context = KrbJni.getSmbKrbContext(initMessage.krbData);
                    byte[] outToken = context.outToken;
                    if (session.sessionId == 0) {
                        session = Session.getNextSession2(session.session2Map, header.getHandler());
                    }
                    String[] split = context.account.split("@");
                    if (split.length >= 2) {
                        session.account = split[0];
                        session.domain = split[1];
                    }
                    session.domainLogin = true;
                    log.debug("domain account :{}", context.account);
                    session.channelSessionKey = context.sessionKey;
                    session.sessionKey = context.sessionKey;
                    short dialect = header.getHandler().negprotInfo.getDialect();
                    if (dialect >= NegprotCall.SMB_3_1_1) {
                        return Mono.error(new MsException(STATUS_NOT_IMPLEMENTED, ""));
                    } else if (dialect >= NegprotCall.SMB_3_0_0) {
                        session.channelSessionKey = CifsUtils.hmacSHA256(session.sessionKey,
                                new byte[]{0, 0, 0, 1}, "SMB2AESCMAC".getBytes(),
                                new byte[]{0, 0}, "SmbSign".getBytes(), new byte[]{0, 0, 0, 0, -128});
                    } else {
                        session.channelSessionKey = session.sessionKey;
                    }
                    NTLMSSPSetupXReply body = new NTLMSSPSetupXReply();
                    SpnegoTokenTargMessage message = new SpnegoTokenTargMessage();
                    message.supportedMech = SpnegoTokenTargMessage.OID_MS_KERBEROS5;
                    Spnego.KrbAqRep krbAqRep = new Spnego.KrbAqRep();
                    krbAqRep.token = outToken;
                    message.token = krbAqRep;
                    reply.getHeader().setSessionId(session.sessionId);
                    body.message = message;
                    reply.setBody(body);
                } catch (Exception e) {
                    log.error("{}", e.getMessage());
                }
                return Mono.just(reply);
            }
        }

        reply.getHeader().status = STATUS_BAD_LOGON_SESSION_STATE;
        return Mono.just(reply);
    }

    @SMB2.Smb2Opt(SMB2_OPCODE.SMB2_TCON)
    public Mono<SMB2Reply> treeConnect(SMB2Header header, Session session, TreeConnectCall call) {
        SMB2Reply reply = new SMB2Reply(header);
        String path = new String(call.path, UTF_16LE);
        int index = path.lastIndexOf('\\');
        if (index > 0) {
            String share = path.substring(index + 1);
            if ("IPC$".equalsIgnoreCase(share)) {
                reply.getHeader().tid = -1;
                TreeConnectReply body = new TreeConnectReply();
                body.setShareType(TreeConnectReply.SMB2_SHARE_TYPE_PIPE);
                body.setMaxAccess(FILE_ALL_ACCESS | STANDARD_RIGHTS_ALL_ACCESS);
                reply.setBody(body);
                return Mono.just(reply);
            }

            String bucket = share.toLowerCase();

            return NFSBucketInfo.getBucketInfoReactive(bucket)
                    .flatMap(bucketInfo -> {
                        if (null == bucketInfo || bucketInfo.isEmpty() || null == bucketInfo.get("fsid") || "0".equals(bucketInfo.get("cifs")) || !CheckUtils.siteCanAccess(bucketInfo)) {
                            reply.getHeader().setStatus(STATUS_ACCESS_DENIED);
                            return Mono.just(reply);
                        }

                        return checkCIFSBucketMount(bucketInfo, session, bucket)
                                .flatMap(pass -> {
                                    if (!pass) {
                                        reply.getHeader().setStatus(STATUS_ACCESS_DENIED);
                                        return Mono.just(reply);
                                    }
                                    String[] specificAccount = {DEFAULT_USER_ID};
                                    Mono<Boolean> preMono = Mono.just(true);
                                    if (session.domainLogin) {
                                        if (session.account != null) {
                                            preMono = pool.getReactive(REDIS_USERINFO_INDEX).exists(session.account)
                                                    .flatMap(exist0 -> {
                                                        if (exist0 > 0) {
                                                            return pool.getReactive(REDIS_USERINFO_INDEX).hgetall(session.account)
                                                                    .flatMap(account -> {
                                                                        String accountType = account.getOrDefault(USER_TYPE, "");
                                                                        String userDomain = account.getOrDefault(ACCOUNT_DOMAIN, "");
                                                                        return Mono.just(AD_ACCOUNT_TYPE.equals(accountType) && userDomain.equalsIgnoreCase(session.domain));
                                                                    });
                                                        }
                                                        return Mono.just(false);
                                                    });
                                        } else {
                                            log.error("session account is null !");
                                            reply.getHeader().setStatus(STATUS_ACCESS_DENIED);
                                            return Mono.just(reply);
                                        }
                                    }
                                    boolean isAnony = (session.flags & 0b1) == 0b1 || "2".equals(bucketInfo.get("guest"));
                                    if (isAnony) {
                                        if (StringUtils.isNotBlank(bucketInfo.get(GUEST_ID))) {
                                            specificAccount[0] = bucketInfo.get(GUEST_ID);
                                        } else {
                                            specificAccount[0] = DEFAULT_USER_ID;
                                        }
                                    }

                                    return preMono.flatMap(f -> {
                                        if (!f) {
                                            reply.getHeader().setStatus(STATUS_ACCESS_DENIED);
                                            return Mono.just(reply);
                                        }
                                        return Mono.just(isAnony)
                                                .flatMap(b -> b? Mono.just(specificAccount[0]) : pool.getReactive(REDIS_USERINFO_INDEX).hget(session.account, USER_DATABASE_NAME_ID))
                                                .map(s3Id -> {
                                                    reply.getHeader().tid = Integer.parseInt(bucketInfo.get("fsid"));
                                                    session.addS3Account(reply.getHeader().tid, s3Id);
                                                    TreeConnectReply body = new TreeConnectReply();
                                                    body.setShareType(TreeConnectReply.SMB2_SHARE_TYPE_DISK);
                                                    body.setMaxAccess(FILE_ALL_ACCESS | STANDARD_RIGHTS_ALL_ACCESS);
                                                    body.setFlags(0x30);
                                                    if (header.getHandler().negprotInfo.getDialect() >= NegprotCall.SMB_3_0_0) {
                                                        body.setCapabilities(SMB2_SHARE_CAP_CONTINUOUS_AVAILABILITY |
                                                                SMB2_SHARE_CAP_SCALEOUT | SMB2_SHARE_CAP_CLUSTER);
                                                    }

                                                    session.bucket = bucket;
                                                    session.bucketInfo = bucketInfo;

                                                    reply.setBody(body);
                                                    return reply;
                                                });
                                    });
                                });
                    });
        }

        reply.getHeader().setStatus(STATUS_ACCESS_DENIED);
        return Mono.just(reply);
    }

    @SMB2.Smb2Opt(value = SMB2_TDIS, allowIPCSession = true)
    public Mono<SMB2Reply> treeDisconnect(SMB2Header header, Session session, TreeDisconnectCall call) {
        CIFSLockClient.treeDisconnect(header);
        ShareAccessClient.treeDisconnect(header.getSessionId(), header.getTid());
        CheckUtils.cifsLeaseOpenCheck().subscribe(b -> {
            if (b) {
                LeaseCache.treeDisconnect(header);
            }
        });
        SMB2Reply reply = new SMB2Reply(header);
        TreeDisConnectReply body = new TreeDisConnectReply();
        reply.setBody(body);
        return Mono.just(reply);
    }

    @SMB2.Smb2Opt(value = SMB2_LOGOFF)
    public Mono<SMB2Reply> logOff(SMB2Header header, Session session, LogOffCall call) {
        CIFSLockClient.logOff(header);
        ShareAccessClient.logOff(header.getSessionId());
        CheckUtils.cifsLeaseOpenCheck().subscribe(b -> {
            if (b) {
                LeaseCache.logOff(header);
            }
        });
        SMB2Reply reply = new SMB2Reply(header);
        LogOffReply body = new LogOffReply();
        reply.setBody(body);

        return Mono.just(reply);
    }
}
