package com.macrosan.utils.asm;

import com.macrosan.constants.ErrorNo;
import com.macrosan.rabbitmq.RabbitMqUtils;
import com.macrosan.utils.msutils.MsException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.impl.AMQChannel;
import lombok.extern.log4j.Log4j2;
import net.sf.cglib.core.ReflectUtils;
import org.objectweb.asm.*;

import java.net.InetAddress;

@Log4j2
public class RabbitmqPublish {
    public static void init() {
        try {
            ClassLoader loader = Channel.class.getClassLoader();
            ClassReader classReader = new ClassReader("com.rabbitmq.client.impl.ChannelN");
            ClassWriter classWriter = new ClassWriter(classReader, ClassWriter.COMPUTE_MAXS);

            ClassVisitor change1 = new ChangeVisitor(classWriter);
            classReader.accept(change1, ClassReader.EXPAND_FRAMES);

            byte[] code = classWriter.toByteArray();
            ReflectUtils.defineClass("com.rabbitmq.client.impl.ChannelN", code, loader);
            log.info("reload ChannelN.class");
        } catch (Exception e) {
            log.error("", e);
        }
    }

    private static class ChangeVisitor extends ClassVisitor {

        ChangeVisitor(ClassVisitor classVisitor) {
            super(Opcodes.ASM9, classVisitor);
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
            if ("basicPublish".equalsIgnoreCase(name) && "(Ljava/lang/String;Ljava/lang/String;ZZLcom/rabbitmq/client/AMQP$BasicProperties;[B)V".equals(desc)) {
                return new MsPublish(super.visitMethod(access, name, desc, signature, exceptions));
            }

            return super.visitMethod(access, name, desc, signature, exceptions);
        }
    }

    /**
     * 修改RSocketRequester中senders、receivers的值
     * 从SynchronizedIntObjectHashMap改为MsIntObjectHashMap;
     */
    private static class MsPublish extends MethodVisitor {

        private MsPublish(MethodVisitor methodVisitor) {
            super(Opcodes.ASM9, methodVisitor);
        }

        @Override
        public void visitCode() {
            //load this
            super.visitVarInsn(Opcodes.ALOAD, 0);
            super.visitMethodInsn(Opcodes.INVOKESTATIC, "com/macrosan/utils/asm/RabbitmqPublish",
                    "checkHealth", "(Ljava/lang/Object;)V", false);
        }

    }

    /**
     * 通过asm在basicPublish前调用
     * basicPublish
     *
     * @param o 调用的channel
     */
    public static void checkHealth(Object o) {
        if (o instanceof AMQChannel) {
            InetAddress address = ((AMQChannel) o).getConnection().getAddress();
            String ip = address.getHostAddress();
            if (!RabbitMqUtils.isLimitSpace(ip)) {
                throw new MsException(ErrorNo.UNKNOWN_ERROR, "moss rabbitmq in " + ip + " is not health");
            }
        }
    }

}
