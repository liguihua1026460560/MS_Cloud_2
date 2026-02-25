package com.macrosan.storage.codec;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;


/**
 * JNA，使用Jerasure库
 *
 * @author gaozhiyuan
 */
public interface JerasureLibrary extends Library {
    interface GFComplete extends Library {

    }

    GFComplete gfComplete = Native.loadLibrary("gf_complete", GFComplete.class);

    JerasureLibrary JERASURE = Native.loadLibrary("Jerasure", JerasureLibrary.class);

    Pointer cauchy_good_general_coding_matrix(int k, int m, int w);

    Pointer jerasure_matrix_to_bitmatrix(int k, int m, int w, Pointer matrix);


    Pointer[] jerasure_smart_bitmatrix_to_schedule(int k, int m,
                                                   int w, Pointer bitmatrix);

    int jerasure_make_decoding_bitmatrix(int k, int m, int w, Pointer bitmatrix, int[] erased, Pointer decoding_matrix, int[] dm_ids);
}
