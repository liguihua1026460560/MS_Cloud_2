package com.macrosan.database.rocksdb.batch;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.rocksdb.MSRocksDB;
import com.macrosan.fs.Allocator.Result;
import com.macrosan.fs.BlockDevice;
import com.macrosan.message.jsonmsg.BlockInfo;
import com.macrosan.utils.msutils.MsException;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatchWithIndex;
import org.rocksdb.WriteOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import static com.macrosan.database.rocksdb.MossMergeOperator.SPACE_SIZE;

@Log4j2
public class BatchWriter {
    public static final long HOOK_UNIT = 16L << 20L;

    String lun;
    //预分配
    private List<Result> preAllocated = new LinkedList<>();
    private long preAllocatedSize = 0L;
    List<Hook> hookList = new LinkedList<>();

    BatchWriter(String lun) {
        this.lun = lun;
    }


    private void preAlloc() {
        BlockDevice blockDevice = BlockDevice.get(lun);
        if (blockDevice == null) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "no such lun :" + lun);
        }
        Result[] results = blockDevice.alloc(HOOK_UNIT);
        for (Result result : results) {
            preAllocated.add(result);
            preAllocatedSize += result.size;
        }


    }

    public Result[] hookData(byte[] data) {
        //以BLOCK_SIZE为单位分配
        int lastSize = BlockDevice.fitBlock(data.length);

        if (preAllocatedSize < lastSize) {
            preAlloc();
        }

        List<Result> resultList = new LinkedList<>();

        ListIterator<Result> iterator = preAllocated.listIterator();
        while (iterator.hasNext() && lastSize > 0) {
            Result preAlloc = iterator.next();
            if (preAlloc.size > lastSize) {
                resultList.add(new Result(preAlloc.offset, lastSize));
                preAlloc.offset += lastSize;
                preAlloc.size -= lastSize;
                preAllocatedSize -= lastSize;
                lastSize = 0;
                break;
            } else {
                //result.size <= lastSize
                resultList.add(new Result(preAlloc.offset, preAlloc.size));
                iterator.remove();
                preAllocatedSize -= preAlloc.size;
                lastSize -= preAlloc.size;
            }
        }

        if (lastSize > 0) {
            throw new MsException(ErrorNo.NO_ENOUGH_SPACE, "NO_ENOUGH_SPACE");
        }

        Result[] results = resultList.toArray(new Result[0]);
        Hook hook = new Hook(data, results);
        hookList.add(hook);
        return results;
    }

    @RequiredArgsConstructor
    static class Hook {
        final byte[] bytes;
        final Result[] results;
    }

    //db.write
    public Mono<Boolean> write(WriteBatchWithIndex batch, WriteOptions writeOptions, RocksDB db) throws IOException {
        if (hookList.isEmpty()) {
            try (WriteBatchWithIndex b0 = batch) {
                db.write(writeOptions, b0);
                return Mono.just(true);
            } catch (Exception e) {
                return Mono.error(e);
            }
        }

        MonoProcessor<Boolean> writeRes = MonoProcessor.create();
        Hook[] hooks = hookList.toArray(new Hook[0]);
        hookList.clear();

        List<Result> flushResults = new LinkedList<>();
        long flushOffset = -1;
        long flushSize = 0L;

        //合并连续的数据块
        for (int i = 0; i < hooks.length; i++) {
            Hook opt = hooks[i];
            for (Result result : opt.results) {
                if (result.offset == flushOffset + flushSize) {
                    flushSize += result.size;
                } else {
                    if (flushSize > 0) {
                        flushResults.add(new Result(flushOffset, flushSize));
                    }

                    flushOffset = result.offset;
                    flushSize = result.size;
                }
            }
        }

        if (flushSize > 0) {
            flushResults.add(new Result(flushOffset, flushSize));
        }

        int i = 0, j = 0;
        int n = 0;
        byte[][] flushBytes = new byte[flushResults.size()][];
        for (Result result : flushResults) {
            byte[] bytes = null;
            int bytesIndex = 0;
            boolean curFull = false;

            for (; i < hooks.length; i++, j = 0) {
                Hook opt = hooks[i];
                for (; j < opt.results.length; j++) {
                    Result cur = opt.results[j];
                    if (opt.results.length == 1 && cur.size == result.size) {
                        bytes = opt.bytes;
                        curFull = true;
                        break;
                    } else {
                        if (bytes == null) {
                            bytes = new byte[(int) result.size];
                        }

                        int jOffset = 0;
                        for (int k = 0; k < j; k++) {
                            jOffset += opt.results[k].size;
                        }
                        int copy = (int) Math.min(Math.min(opt.bytes.length, opt.bytes.length - jOffset), cur.size);
                        System.arraycopy(opt.bytes, jOffset, bytes, bytesIndex, copy);
                        bytesIndex += cur.size;

                        if (bytes.length == bytesIndex) {
                            curFull = true;
                            break;
                        }
                    }
                }

                if (curFull) {
                    break;
                }
            }

            //next
            if (i < hooks.length) {
                j++;

                if (j == hooks[i].results.length) {
                    j = 0;
                    i++;
                }
            }

            flushBytes[n++] = bytes;
        }

        assert i == hooks.length;
        assert j == 0;

        n = 0;
        Mono[] monos = new Mono[flushBytes.length];
        for (Result result : flushResults) {
            byte[] data = flushBytes[n];
            monos[n++] = BlockDevice.get(lun).getChannel().write(data, result);
        }

        Flux.merge(monos).subscribe(s -> {
        }, e -> {
            batch.close();
            writeRes.onError((Throwable) e);
        }, () -> {
            try {
                for (Result result : flushResults) {
                    List<byte[]> list = BlockInfo.getUpdateValue(result.offset, result.size, "upload");
                    for (int k = 0; k < list.size(); k++) {
                        String key = BlockInfo.getFamilySpaceKey((result.offset / SPACE_SIZE) + k);
                        batch.merge(MSRocksDB.getColumnFamily(lun), key.getBytes(), list.get(k));
                    }
                }

                db.write(writeOptions, batch);
                batch.close();
                writeRes.onNext(true);
            } catch (Exception e) {
                batch.close();
                writeRes.onError(e);
            }
        });

        return writeRes;
    }
}
