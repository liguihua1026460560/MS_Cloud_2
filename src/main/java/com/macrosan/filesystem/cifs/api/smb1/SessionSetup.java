package com.macrosan.filesystem.cifs.api.smb1;

import com.macrosan.filesystem.cifs.SMB1;
import com.macrosan.filesystem.cifs.SMB1.SMB1Reply;
import com.macrosan.filesystem.cifs.SMB1Header;
import com.macrosan.filesystem.cifs.SMB2;
import com.macrosan.filesystem.cifs.call.smb1.NTLMSSPSetupXCall;
import com.macrosan.filesystem.cifs.call.smb1.TreeConnectXCall;
import com.macrosan.filesystem.cifs.reply.smb1.EmptyReply;
import com.macrosan.filesystem.cifs.reply.smb1.NTLMSSPSetupXReply;
import com.macrosan.filesystem.cifs.reply.smb1.TreeConnectXReply;
import com.macrosan.filesystem.cifs.types.NTLMSSP;
import com.macrosan.filesystem.cifs.types.Session;
import com.macrosan.filesystem.cifs.types.Spnego.SpnegoMessage;
import com.macrosan.filesystem.cifs.types.Spnego.SpnegoTokenTargMessage;
import com.macrosan.filesystem.nfs.NFSBucketInfo;
import com.macrosan.filesystem.utils.CifsUtils;
import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Map;

import static com.macrosan.filesystem.FsConstants.NTStatus.*;
import static com.macrosan.filesystem.cifs.SMB1.SMB1_OPCODE.SMB_TCONX;
import static com.macrosan.filesystem.cifs.call.smb1.TreeConnectXCall.TCONX_FLAG_EXTENDED_RESPONSE;
import static com.macrosan.filesystem.cifs.reply.smb1.TreeConnectXReply.*;
import static com.macrosan.filesystem.cifs.types.NTLMSSP.*;
import static java.nio.charset.StandardCharsets.UTF_16LE;

@Log4j2
public class SessionSetup {
    private static final SessionSetup instance = new SessionSetup();

    private SessionSetup() {
    }

    public static SessionSetup getInstance() {
        return instance;
    }

    @SMB1.Smb1Opt(SMB_TCONX)
    public Mono<SMB1.SMB1Reply> treeConnectX(SMB1Header header, Session session, TreeConnectXCall call) {
        SMB1Reply reply = new SMB1Reply(header);
        SMB1Header replyHeader = reply.getHeader();

        if (session == null) {
            replyHeader.status = STATUS_NETWORK_SESSION_EXPIRED;
            return Mono.just(reply);
        }

        String path = new String(call.path);

        if (Arrays.equals(call.service, TreeConnectXCall.DISK) || Arrays.equals(call.service, TreeConnectXCall.ANY)) {
            int index = path.lastIndexOf('\\');
            if (index > 0) {
                String bucket = path.substring(index + 1);
                bucket = bucket.toLowerCase();
                if (SMB2.IPC.equalsIgnoreCase(bucket)) {
                    replyHeader.tid = (short) -1;
                    TreeConnectXReply body = new TreeConnectXReply();

                    body.xOffset = 0;
                    body.xOpcode = -1;
                    body.optionalSupport = SMB_SUPPORT_SEARCH_BITS;
                    if ((call.flags & TCONX_FLAG_EXTENDED_RESPONSE) != 0) {
                        body.wordCount = 7;
                        body.accessRights = FILE_ALL_ACCESS | STANDARD_RIGHTS_ALL_ACCESS;
                        body.guestAccessRights = 0;
                    } else {
                        body.wordCount = 3;
                    }

                    body.service = TreeConnectXCall.NAME;
                    body.fileSystem = "".toCharArray();

                    replyHeader.status = 0;
                    reply.setBody(body);
                    return Mono.just(reply);
                } else {
                    Map<String, String> bucketInfo = NFSBucketInfo.getBucketInfo(bucket);

                    if (bucketInfo != null && !bucketInfo.isEmpty()) {
//                        session.bucket = bucket;
//                        session.bucketInfo = bucketInfo;

                        replyHeader.tid = Short.parseShort(bucketInfo.get("fsid"));
                        TreeConnectXReply body = new TreeConnectXReply();
                        body.xOffset = 0;
                        body.xOpcode = -1;
                        body.optionalSupport = SMB_SUPPORT_SEARCH_BITS;
                        if ((call.flags & TCONX_FLAG_EXTENDED_RESPONSE) != 0) {
                            body.wordCount = 7;
                            body.accessRights = FILE_ALL_ACCESS | STANDARD_RIGHTS_ALL_ACCESS;
                            body.guestAccessRights = 0;
                        } else {
                            body.wordCount = 3;
                        }

                        body.service = TreeConnectXCall.DISK;
                        body.fileSystem = "NTFS".toCharArray();
                        replyHeader.status = 0;
                        reply.setBody(body);
                        return Mono.just(reply);
                    }
                }
            }
        }

        replyHeader.status = STATUS_ACCESS_DENIED;
        return Mono.just(reply);
    }

    @SMB1.Smb1Opt(value = SMB1.SMB1_OPCODE.SMB_SESSSETUPX, allowNoSession = true)
    public Mono<SMB1Reply> ntlmsspSetupX(SMB1Header header, Session session, NTLMSSPSetupXCall call) {
        NTLMSSP.NTLMSSPMessage msg = call.getNtlmsspMessage();
        SMB1Reply reply = new SMB1Reply(header);
        SMB1Header replyHeader = reply.getHeader();

        if (msg instanceof SpnegoMessage) {
            SpnegoMessage spnegoMsg = (SpnegoMessage) msg;
            msgHandler(header, replyHeader, reply, session, spnegoMsg.token);
            if (reply.getBody() instanceof NTLMSSPSetupXReply) {
                NTLMSSPSetupXReply replyBody = (NTLMSSPSetupXReply) reply.getBody();
                SpnegoTokenTargMessage resMsg = new SpnegoTokenTargMessage();

                if (replyBody.message == null) {
                    resMsg.res = SpnegoTokenTargMessage.SPNEGO_ACCEPT_COMPLETED;
                    replyBody.setMessage(resMsg);
                } else {
                    resMsg.supportedMech = SpnegoTokenTargMessage.GSS_NTLM_MECHANISM;
                    resMsg.token = replyBody.message;
                    resMsg.res = SpnegoTokenTargMessage.SPNEGO_ACCEPT_INCOMPLETE;
                    replyBody.setMessage(resMsg);
                }
            }
        } else {
            msgHandler(header, replyHeader, reply, session, msg);
        }
        return Mono.just(reply);
    }

    public void msgHandler(SMB1Header header, SMB1Header replyHeader, SMB1Reply reply, Session session, NTLMSSPMessage msg) {
        if (msg instanceof NTLMSSP.NTLMNegotiate) {
            //session == noSession
            if (session.uid == 0) {
                session = Session.getNextSession1(session.session1Map);
                NTLMSSPSetupXReply body = new NTLMSSPSetupXReply();
                body.xOpcode = -1;
                body.action = 0;
                body.xOffset = 0;
                NTLMSSP.NTLMChallenge challenge = new NTLMChallenge();
                challenge.targetName = "MOSS";
                challenge.serverChallenge = session.serverChallenge;
                challenge.version = 0x0f00000000000106L;
                challenge.targetInfo.put(TargetKey.NbComputerName, "MOSS");
                challenge.targetInfo.put(TargetKey.NbDomainName, "MOSS");
                challenge.targetInfo.put(TargetKey.DnsDomainName, ".com");
                challenge.targetInfo.put(TargetKey.DnsComputerName, "moss.com");
                challenge.targetInfo.put(TargetKey.Timestamp, CifsUtils.nttime(System.currentTimeMillis()));

                challenge.flags = NTLMSSP_NEGOTIATE_EXTENDED_SESSIONSECURITY | NTLMSSP_NEGOTIATE_128
                        | NTLMSSP_NEGOTIATE_KEY_EXCH | NTLMSSP_NEGOTIATE_VERSION | NTLMSSP_REQUEST_TARGET
                        | NTLMSSP_NEGOTIATE_SEAL | NTLMSSP_NEGOTIATE_NTLM | NTLMSSP_NEGOTIATE_TARGET_INFO
                        | NTLMSSP_TARGET_TYPE_SERVER;

                if ((((NTLMNegotiate) msg).flags & NTLMSSP_NEGOTIATE_UNICODE) != 0) {
                    challenge.flags |= NTLMSSP_NEGOTIATE_UNICODE;
                }

                body.message = challenge;
                replyHeader.uid = session.uid;
                replyHeader.status = STATUS_INVALID_DEVICE_REQUEST;
                reply.setBody(body);
                return;
            }
        } else if (msg instanceof NTLMSSP.NTLMAuth) {
            if (session.uid == 0) {
                log.error("no  session {}", header);
                replyHeader.status = STATUS_BAD_LOGON_SESSION_STATE;
                return;
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

            String passwd = "samb";
            //TODO 到redis表3找密码
            if (!auth.checkAuth(session.domain, session.account, passwd, session.serverChallenge)) {
                replyHeader.status = STATUS_LOGON_FAILURE;
                reply.setBody(EmptyReply.DEFAULT);
                return;
            }

            NTLMSSPSetupXReply body = new NTLMSSPSetupXReply();
            body.xOpcode = -1;
            body.action = 0;
            body.xOffset = 0;
            body.message = null;

            replyHeader.status = 0;
            reply.setBody(body);
            return;
        }

        replyHeader.status = STATUS_BAD_LOGON_SESSION_STATE;
    }
}
