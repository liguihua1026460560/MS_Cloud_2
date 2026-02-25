package com.macrosan.utils.asm;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cglib.core.ReflectUtils;

/**
 * @author zhaoyang
 * @date 2025/10/13
 **/
@Slf4j
public class OpenSSLMessageDigestNativeRepair {
    public static void init() {
        try {
            ClassPool pool = ClassPool.getDefault();
            CtClass cc = pool.get("de.sfuhrm.openssl4j.OpenSSLMessageDigestNative");
            CtMethod method = cc.getDeclaredMethod("engineUpdate", new CtClass[]{pool.get("java.nio.ByteBuffer")});
            String newMethodBody =
                    "{\n" +
                            "    if ($1.hasRemaining()) {\n" +
                            "        int remaining = $1.remaining();\n" +
                            "        int pos = $1.position();\n" +
                            "        if ($1.isDirect()) {\n" +
                            "            $0.nativeUpdateWithByteBuffer($0.context, $1, pos, remaining);\n" +
                            "            $1.position($1.position() + remaining);\n" +
                            "        } else {\n" +
                            "            byte[] array;\n" +
                            "            int ofs = $1.arrayOffset();\n" +
                            "            if ($1.hasArray()) {\n" +
                            "                array = $1.array();\n" +
                            "                $0.nativeUpdateWithByteArray($0.context, array, ofs + pos, remaining);\n" +
                            "                $1.position(pos + remaining);\n" +
                            "            } else {\n" +
                            "                array = new byte[remaining];\n" +
                            "                $1.get(array);\n" +
                            "                $0.nativeUpdateWithByteArray($0.context, array, 0, array.length);\n" +
                            "            }\n" +
                            "        }\n" +
                            "    }\n" +
                            "}";
            method.setBody(newMethodBody);
            ClassLoader loader = OpenSSLMessageDigestNativeRepair.class.getClassLoader();
            ReflectUtils.defineClass("de.sfuhrm.openssl4j.OpenSSLMessageDigestNative", cc.toBytecode(), loader);
            log.info("reload OpenSSLMessageDigestNative.class");
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
