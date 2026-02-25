package com.macrosan.utils.asm;

import lombok.extern.log4j.Log4j2;
import net.sf.cglib.core.ReflectUtils;
import org.objectweb.asm.*;

/**
 * @author gaozhiyuan
 */
@Log4j2
public class RSocketRequesterRepair {
    private static class ChangeVisitor extends ClassVisitor {

        ChangeVisitor(ClassVisitor classVisitor) {
            super(Opcodes.ASM9, classVisitor);
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
            if ("handleRequestResponse".equalsIgnoreCase(name)) {
                return new MsMethod2(super.visitMethod(access, name, desc, signature, exceptions));
            }

            //修改io.rsocket.RSocketRequester的构造函数
            if ("<init>".equalsIgnoreCase(name)) {
                return new MsMethod(super.visitMethod(access, name, desc, signature, exceptions));
            }

            return super.visitMethod(access, name, desc, signature, exceptions);
        }
    }

    /**
     * 修改RSocketRequester中senders、receivers的值
     * 从SynchronizedIntObjectHashMap改为MsIntObjectHashMap;
     */
    private static class MsMethod extends MethodVisitor {

        private MsMethod(MethodVisitor methodVisitor) {
            super(Opcodes.ASM9, methodVisitor);
        }

        @Override
        public void visitMethodInsn(final int opcode, final String owner, final String name, final String descriptor, final boolean isInterface) {
            if ("io/rsocket/internal/SynchronizedIntObjectHashMap".equalsIgnoreCase(owner) &&
                    "<init>".equalsIgnoreCase(name)) {
                super.visitMethodInsn(opcode, "com/macrosan/utils/asm/MsIntObjectHashMap", name, descriptor, isInterface);
            } else {
                super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
            }
        }

        @Override
        public void visitTypeInsn(final int opcode, final String type) {
            //修改io.rsocket.RSocketRequester的构造函数
            if ("io/rsocket/internal/SynchronizedIntObjectHashMap".equalsIgnoreCase(type) && Opcodes.NEW == opcode) {
                super.visitTypeInsn(opcode, "com/macrosan/utils/asm/MsIntObjectHashMap");
            } else {
                super.visitTypeInsn(opcode, type);
            }
        }
    }

    /**
     * 替换 handleRequestResponse
     */
    private static class MsMethod2 extends MethodVisitor {
        private MsMethod2(MethodVisitor methodVisitor) {
            super(Opcodes.ASM9, methodVisitor);
        }

        @Override
        public void visitMethodInsn(final int opcode, final String owner, final String name, final String descriptor, final boolean isInterface) {
            //替换UnicastMonoProcessor，修改为MonoProcessor
            if ("io/rsocket/internal/UnicastMonoProcessor".equalsIgnoreCase(owner)) {
                if ("create".equalsIgnoreCase(name)) {
                    super.visitMethodInsn(opcode, "reactor/core/publisher/MonoProcessor", "create", "()Lreactor/core/publisher/MonoProcessor;", isInterface);
                } else {
                    super.visitMethodInsn(opcode, "reactor/core/publisher/MonoProcessor", name, descriptor, isInterface);
                }
            } else {
                super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
            }
        }
    }

    public static void init() {
        try {
            ClassLoader loader = RSocketRequesterRepair.class.getClassLoader();
            ClassReader classReader = new ClassReader("io.rsocket.RSocketRequester");
            ClassWriter classWriter = new ClassWriter(classReader, ClassWriter.COMPUTE_MAXS);

            ClassVisitor change1 = new ChangeVisitor(classWriter);
            classReader.accept(change1, ClassReader.EXPAND_FRAMES);

            byte[] code = classWriter.toByteArray();
            ReflectUtils.defineClass("io.rsocket.RSocketRequester", code, loader);
            log.info("reload RSocketRequester.class");
        } catch (Exception e) {
            log.error("", e);
        }
    }
}
