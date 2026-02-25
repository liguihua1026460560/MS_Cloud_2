package com.macrosan.utils.asm;


import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import lombok.extern.log4j.Log4j2;
import org.springframework.cglib.core.ReflectUtils;

@Log4j2
public class HttpServerResponseImplRepair {

    public static void init() {
        try {
            // 创建类池并获取目标类
            ClassPool pool = ClassPool.getDefault();
            CtClass cc = pool.get("io.vertx.core.http.impl.HttpServerResponseImpl");

            // 获取 end 方法
            CtMethod method = cc.getDeclaredMethod("end", new CtClass[]{
                    pool.get("io.vertx.core.buffer.Buffer"),
                    pool.get("io.netty.channel.ChannelPromise")
            });

            // 修改方法体，将代码放入同步块中
            String body =
                    "synchronized(this.conn) {"
                    + "    synchronized (Thread.currentThread()) {"
                    + "        if (this.written) {"
                    + "            throw new IllegalStateException(\"Response has already been written\");"
                    + "        }"
                    + "        io.netty.buffer.ByteBuf data = $1.getByteBuf();"
                    + "        this.bytesWritten += data.readableBytes();"
                    + "        io.netty.handler.codec.http.HttpObject msg;"
                    + "        if (!this.headWritten) {"
                    + "            this.prepareHeaders(this.bytesWritten);"
                    + "            msg = new io.vertx.core.http.impl.AssembledFullHttpResponse(this.head, this.version, this.status, this.headers, data, this.trailingHeaders);"
                    + "        } else {"
                    + "            msg = new io.vertx.core.http.impl.AssembledLastHttpContent(data, this.trailingHeaders);"
                    + "        }"
                    + "        this.conn.writeToChannel(msg, $2);"
                    + "        this.written = true;"
                    + "        this.conn.responseComplete();"
                    + "        if (this.bodyEndHandler != null) {"
                    + "            this.bodyEndHandler.handle(null);"
                    + "        }"
                    + "        if (!this.closed && this.endHandler != null) {"
                    + "            this.endHandler.handle(null);"
                    + "        }"
                    + "        if (!this.keepAlive) {"
                    + "            this.closeConnAfterWrite();"
                    + "            this.closed = true;"
                    + "        }"
                    + "    }"
                    + "}";
            method.setBody(body);
            // 将修改后的类写回文件系统
            ClassLoader loader = HttpServerResponseImplRepair.class.getClassLoader();
            ReflectUtils.defineClass("io.vertx.core.http.impl.HttpServerResponseImpl", cc.toBytecode(), loader);
            log.info("reload HttpServerResponseImpl.class");
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
