package com.macrosan.utils.asm;

import lombok.extern.log4j.Log4j2;
import org.objectweb.asm.*;
import org.springframework.cglib.core.ReflectUtils;

/**
 * 修改io.rsocket.internal.jctools.queues.BaseMpscLinkedArrayQueue的字节码
 * BaseMpscLinkedArrayQueue中并发调用poll方法有概率导致死循环
 * 在原始的pool方法前加synchronized解决这个bug
 *
 * @author gaozhiyuan
 */
@Log4j2
public class BaseMpscLinkedArrayQueueRepair {
    private static class ChangeVisitor extends ClassVisitor {

        ChangeVisitor(ClassVisitor classVisitor) {
            super(Opcodes.ASM5, classVisitor);
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
            //对poll方法设置synchronized标志(access 0x21)
            if ("poll".equalsIgnoreCase(name)) {
                return super.visitMethod(0x21, name, desc, signature, exceptions);
            }

            return super.visitMethod(access, name, desc, signature, exceptions);
        }
    }

    public static void init() {
        try {
            ClassReader classReader = new ClassReader("io.rsocket.internal.jctools.queues.BaseMpscLinkedArrayQueue");
            ClassWriter classWriter = new ClassWriter(classReader, ClassWriter.COMPUTE_MAXS);
            ClassVisitor change = new ChangeVisitor(classWriter);
            classReader.accept(change, ClassReader.EXPAND_FRAMES);
            ClassLoader loader = BaseMpscLinkedArrayQueueRepair.class.getClassLoader();
            byte[] code = classWriter.toByteArray();
            ReflectUtils.defineClass("io.rsocket.internal.jctools.queues.BaseMpscLinkedArrayQueue", code, loader);
            log.info("reload BaseMpscLinkedArrayQueue.class");
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
