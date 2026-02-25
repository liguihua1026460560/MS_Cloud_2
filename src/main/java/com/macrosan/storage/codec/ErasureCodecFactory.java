package com.macrosan.storage.codec;

import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

import java.util.LinkedList;
import java.util.List;

import static com.macrosan.storage.codec.JerasureLibrary.JERASURE;

/**
 * @author gaozhiyuan
 */
class ErasureCodecFactory {
    private static void free(Pointer p) {
        long peer = Pointer.nativeValue(p);
        Native.free(peer);
        Pointer.nativeValue(p, 0);
    }

    static int[][] loadEncodeScheduler(int k, int m, int[] index) {
        Pointer matrix = JERASURE.cauchy_good_general_coding_matrix(k, m, 8);
        Pointer bitMatrix = JERASURE.jerasure_matrix_to_bitmatrix(k, m, 8, matrix);
        Pointer[] schedulerPointer;
        if (index == null) {
            schedulerPointer = JERASURE.jerasure_smart_bitmatrix_to_schedule(k, m, 8, bitMatrix);
        } else {
            Pointer decodeBitMatrix = new Memory(k * k * 8 * 8 * 4);
            int[] tmp = new int[k];
            int[] erased = new int[k + m];
            for (int i = 0; i < k + m; i++) {
                if (index[i] == 0) {
                    erased[i] = 1;
                }
            }

            JERASURE.jerasure_make_decoding_bitmatrix(k, m, 8, bitMatrix, erased, decodeBitMatrix, tmp);
            schedulerPointer = JERASURE.jerasure_smart_bitmatrix_to_schedule(k, k, 8, decodeBitMatrix);
            free(decodeBitMatrix);
        }

        free(matrix);
        free(bitMatrix);

        List<int[]> schedulerList = new LinkedList<>();

        for (int i = 0; i < schedulerPointer.length - 1; i++) {
            int[] scheduler = new int[5];
            schedulerPointer[i].read(0, scheduler, 0, 5);
            free(schedulerPointer[i]);

            if (scheduler[0] < 0) {
                break;
            }

            scheduler[2] -= k;
            schedulerList.add(scheduler);
        }

        return schedulerList.toArray(new int[0][]);
    }
}
