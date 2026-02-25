package com.macrosan.rsocket.filesystem;

import com.macrosan.constants.SysConstants;
import com.macrosan.localtransport.LunManager;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.json.JsonObject;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.spi.FileSystemProvider;
import java.util.EnumSet;

import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.*;

/**
 * ObjectFile
 *
 * @author liyixin
 * @date 2019/8/29
 */
@Log4j2
public class ObjectFile implements Closeable {

    private static final Path EMPTY_PATH = Paths.get("/");

    private static final EnumSet<StandardOpenOption> WRITE_OPTION = EnumSet.of(DSYNC, WRITE, CREATE_NEW);
    private static final EnumSet<StandardOpenOption> READ_OPTION = EnumSet.of(DSYNC, READ);

    private static final StandardCopyOption[] COPY_OPTIONS = {REPLACE_EXISTING, ATOMIC_MOVE};

    private FileChannel channel;

    private FileChannel originalChannel;

    private Path path;

    private Path tmpPath = EMPTY_PATH;

    private LunManager.LunInfo lunInfo;

    private ByteBuffer[] bufArray = new ByteBuffer[64];

    private int index;

    @Setter
    private String aclStr = "{}";

    private static final FileSystemProvider PROVIDER = FileSystems.getDefault().provider();

    public void open(String path) throws IOException {
        closeIfExport();
        lunInfo.increase();

        this.path = Paths.get(path);
        final Path directory = this.path.getParent();
        if (!Files.exists(directory)) {
            Files.createDirectories(directory);
        }

        try {
            channel = PROVIDER.newFileChannel(this.path, WRITE_OPTION);

        } catch (IOException e) {
            tmpPath = Paths.get(path + getRequestId());
            channel = PROVIDER.newFileChannel(tmpPath, WRITE_OPTION);
            originalChannel = PROVIDER.newFileChannel(this.path, READ_OPTION);
            lunInfo.increase();
        }
    }

    public void write(ByteBuffer buf) throws IOException {
        if (index >= bufArray.length) {
            flush();
        }

        bufArray[index++] = buf;
    }

    public void flush() throws IOException {
        getChannel().write(bufArray, 0, index);
        index = 0;
    }

    public void setLunName(String lunName) {
        this.lunInfo = LunManager.getLunInfo(lunName);

    }

    public boolean exists() {
        return tmpPath != EMPTY_PATH;
    }

    @Override
    public void close() {
        close(this.channel);
        this.channel = null;

        close(this.originalChannel);
        this.originalChannel = null;
    }

    public void closeInException(String cause) {
        close();
        try {
            if (exists()) {
                if (Files.exists(tmpPath)) {
                    Files.delete(tmpPath);
                    log.error("delete tmp file : {} in exception, cause {}", tmpPath.toString(), cause);
                }
            } else if (Files.exists(path)) {
                Files.delete(path);
                log.error("delete file : {} in exception, cause {}", path.toString(), cause);
            }
        } catch (IOException e) {
            log.error(e);
        }
    }

    private void close(FileChannel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                log.error(e);
            } finally {
                lunInfo.decrease();
            }
        }
    }

    public void complete() {
        try {
            write(ByteBuffer.wrap(aclStr.getBytes()));
            flush();
            if (exists()) {
                Files.move(tmpPath, path, COPY_OPTIONS);
            }
        } catch (IOException e) {
            log.error(e);
        } finally {
            close();
        }
    }

    public FileChannel getChannel() {
        closeIfExport();
        return channel;
    }

    private void closeIfExport() {
        if (lunInfo.isAvailable()) {
            return;
        }

        close();

        throw new MsException(-1104, "lun " + lunInfo.getLunName() + " is not available");
    }

    public JsonObject getSysMeta() {
        return readMetaData(0, 1024);
    }

    public JsonObject getUserMeta() {
        final JsonObject sysMeta = getSysMeta();
        long usrMetaStartIndex = sysMeta.containsKey("fileType") ?
                SysConstants.META_SYS_MAX_SIZE :
                Long.parseLong(sysMeta.getString("Content-Length")) + SysConstants.META_SYS_MAX_SIZE;

        int userMetaSize = sysMeta.containsKey("userMetasize") ?
                Integer.parseInt(sysMeta.getString("userMetasize")) : 2 * 1024;

        return readMetaData(usrMetaStartIndex, userMetaSize);

    }

    public JsonObject getAcl() {
        final JsonObject sysMeta = getSysMeta();
        if (sysMeta == null || !sysMeta.containsKey("acl")) {
            return null;
        }
        long index = sysMeta.containsKey("fileType") ?
                SysConstants.META_SYS_MAX_SIZE :
                Long.parseLong(sysMeta.getString("Content-Length")) + SysConstants.META_SYS_MAX_SIZE;

        int userMetaSize = sysMeta.containsKey("userMetasize") ?
                Integer.parseInt(sysMeta.getString("userMetasize")) : 2 * 1024;

        index = index + userMetaSize;

        final JsonObject acl = readMetaData(index, 0);
        if (acl == null) {
            return null;
        }

        acl.put("acl", sysMeta.getString("acl"))
                .put("owner", sysMeta.getString("owner"));

        close(this.originalChannel);
        this.originalChannel = null;
        return acl;
    }

    private JsonObject readMetaData(long startPos, int size) {
        if (!exists()) {
            return null;
        }

        closeIfExport();

        try {
            if (size == 0) {
                size = (int) (originalChannel.size() - startPos);
                if (size <= 0) {
                    return null;
                }
            }
            originalChannel.position(startPos);
            ByteBuffer buf = ByteBuffer.allocate(size);
            originalChannel.read(buf);
            return new JsonObject(new String(buf.array()));
        } catch (IOException e) {
            log.error(e);
            close(this.originalChannel);
            this.originalChannel = null;
            return null;
        }
    }
}
