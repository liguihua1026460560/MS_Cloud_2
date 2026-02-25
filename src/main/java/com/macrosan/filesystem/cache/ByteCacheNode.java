package com.macrosan.filesystem.cache;

import com.macrosan.utils.functional.Tuple2;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;

import java.util.LinkedList;
import java.util.List;

import static com.macrosan.filesystem.cache.WriteCache.MAX_TRUNK_CACHE_SIZE;

@Log4j2
@ToString(exclude = "data")
public class ByteCacheNode {
    public long offset;
    public int size;
    List<Tuple2<Long, Integer>> pages = new LinkedList<>();
    public byte[] data;

    private ByteCacheNode(long offset, int size, byte[] data) {
        this.offset = offset;
        this.size = size;
        this.data = data;
    }

    public static ByteCacheNode alloc(long offset, int size) {
        byte[] data = BytesPoolNode.allocate();
        if(data == null) {
            return null;
        }

        return new ByteCacheNode(offset, size, data);
    }

    public void updateOffset(long offset) {
        this.offset = Math.min(this.offset, offset);
    }


    public void put(long offset, byte[] bytes) {
        int start = (int) (offset % MAX_TRUNK_CACHE_SIZE);
        System.arraycopy(bytes, 0, data, start, bytes.length);
        addPage(new Tuple2<>(offset, bytes.length));
        size += bytes.length;
        compress();
    }

    private void addPage(Tuple2<Long, Integer> page) {
        int idx = 0;
        boolean addLast = true;
        for (int i = 0; i < pages.size(); i++) {
            if (pages.get(i).var1 > page.var1) {
                idx = i;
                addLast = false;
                break;
            }
        }
        if (addLast) {
            pages.add(page);
        } else {
            pages.add(idx, page);
        }
    }

    private void compress() {
        Tuple2<Long, Integer> lastPage = null;
        int lastIndex = 0;
        for (int i = 0; i < pages.size(); i++) {
            if (lastPage == null) {
                lastPage = pages.get(i);
                lastIndex = i;
            } else {
                //========
                //        ============
                Tuple2<Long, Integer> curPage = pages.get(i);
                if (lastPage.var1 + lastPage.var2 == curPage.var1) {
                    lastPage.var2 += curPage.var2;
                    pages.set(lastIndex, lastPage);
                    pages.remove(i);
                    i--;
                }
                //========
                //              ============
                else if (lastPage.var1 + lastPage.var2 < curPage.var1) {
                    pages.set(lastIndex, lastPage);
                    lastPage = curPage;
                    lastIndex = i;
                } else if (lastPage.var1 + lastPage.var2 > curPage.var1) {
                    //================
                    //  ============
                    if (lastPage.var1 + lastPage.var2 >= curPage.var1 + curPage.var2) {
                        this.size -= curPage.var2;
                        pages.remove(i);
                        i--;
                    }
                    //================
                    //   ====================
                    else if (lastPage.var1 + lastPage.var2 < curPage.var1 + curPage.var2) {
                        long offset = lastPage.var1 + lastPage.var2;
                        int size = (int) (curPage.var1 + curPage.var2 - offset);
                        lastPage.var2 += size;
                        this.size -= (curPage.var2 - size);
                        pages.set(lastIndex, lastPage);
                        pages.remove(i);
                        i--;
                    }
                }
            }
        }
    }
}