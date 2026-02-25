package com.macrosan.filesystem.ftp;

import com.macrosan.utils.regex.Pattern;
import io.vertx.reactivex.core.buffer.Buffer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;

@Slf4j
@Data
public class FTPRequest {
    public Command command;
    public List<String> args;

    public FTPRequest(String str) {
        String[] strs = str.split(" ", 2);
        this.command = Command.valueOf(strs[0].toUpperCase(Locale.ROOT));
        this.args = new ArrayList<>();
        if (this.command.equals(Command.MFMT)) {
            // (?i)       忽略大小写 (匹配 mfmt 或 MFMT)
            // ^MFMT      以 MFMT 开头
            // \s+        匹配中间的一个或多个空格
            // (\d{14})   匹配14位数字（日期）
            // \s+        匹配时间后的空格
            // (.*)$      匹配剩余所有字符（文件名）
            String regex = "(?i)^MFMT\\s+(\\d{14})\\s+(.*)$";

            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(str.trim());

            if (matcher.find()) {
                String timeStr = matcher.group(1);
                String fileName = matcher.group(2);
                this.args.add(timeStr);
                this.args.add(fileName);
            }
        } else {
            for (int i = 1; i < strs.length; i++) {
                this.args.add(strs[i]);
            }
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface FTPOpt {
        Command value();
    }

    public enum Command {
        //rfc959
        // 访问控制命令
        USER,   // 用户名
        PASS,   // 密码
        ACCT,   // 账户信息 (不是必要的，暂不实现)
        CWD,    // 修改工作目录 work dir
        CDUP,   // work dir -> 父目录
        SMNT,   // 切换文件系统 (在MOSS中没有意义，不需要实现)
        REIN,   // 重新初始化session (不是必要的，暂不实现)
        QUIT,   // 退出

        // 传输参数命令
        PORT,   // 数据端口 (主动模式，服务器主动连客户端端口，暂不支持)
        PASV,   // 被动模式
        TYPE,   // 文件类型  A E I L
        STRU,   // 文件结构 F R P
        MODE,   // 传输模式 S B C

        // 服务命令
        RETR,   // 下载文件
        STOR,   // 上传文件
        STOU,   // 上传文件，但文件名随机
        APPE,   // 追加文件
        ALLO,   // 请求服务器分配足够的空间
        REST,   // 设置偏移量，断点下载、上传等功能需要支持

        RNFR,   // 重命名起始
        RNTO,   // 重命名目标
        ABOR,   // 中止

        DELE,   // 删除文件
        RMD,    // 删除目录
        MKD,    // 创建目录
        PWD,    // 打印工作目录
        LIST,   // 列出文件
        NLST,   // 列出文件名
        SITE,   // 站点特定命令
        SYST,   // 系统类型
        STAT,   // 状态
        HELP,   // 帮助
        NOOP,   // 空操作

        // 扩展命令
        //rfc2389
        FEAT,   // 功能列表 
        OPTS,   // 选项
        //rfc2228
        AUTH,   // 开启数据加密
        ADAT,
        PBSZ,   // 保护缓冲区大小
        PROT,   // 数据通道保护级别
        CCC,
        MIC, CONF, ENC,
        //rfc3659
        MDTM,   // 文件修改时间
        SIZE,   // 文件大小
        TVFS,
        MLST,   // 列出文件信息
        MLSD,   // 列出目录详细信息
        //rfc1639
        LPRT, //PORT 长地址版本(ipv6)
        LPSV, //PASV 长地址版本(ipv6)
        //rfc2640
        LANG,   // 语言
        // rfc2428
        EPRT,   // 扩展主动模式
        EPSV,   // 扩展被动模式
        //rfc775
        XCUP, //旧命令的名称，和没有X的命令相同
        XCWD,
        XMKD,
        XPWD,
        XRMD,
        MFMT,
    }
}
