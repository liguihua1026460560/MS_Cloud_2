package com.macrosan.filesystem.nfs;

import com.macrosan.filesystem.FsConstants;
import com.macrosan.utils.msutils.MsException;
import lombok.extern.log4j.Log4j2;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

@Log4j2
public class NFSException extends MsException {
    public boolean nfsError;

    public NFSException(int errCode, String message) {
        super(errCode, message);
        nfsError = ALL_NFS_ERROR_SET.contains(errCode);
    }

    public static final IntHashSet ALL_NFS_ERROR_SET;

    static {
        ALL_NFS_ERROR_SET = new IntHashSet();
        for (Field field : FsConstants.NfsErrorNo.class.getDeclaredFields()) {
            int modifiers = field.getModifiers();
            if (Modifier.isStatic(modifiers) && Modifier.isFinal(modifiers) && field.getType() == int.class) {
                field.setAccessible(true);
                try {
                    int error = (int) field.get(null);
                    ALL_NFS_ERROR_SET.add(error);
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        }
    }
}
