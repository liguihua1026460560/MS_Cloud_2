package com.macrosan.database.rocksdb.batch;

import java.util.Arrays;

public class ValueGetter {
    byte[][] parameters;
    int[] indexes;

    public ValueGetter(String... parameters) {
        this.parameters = new byte[parameters.length][];
        indexes = new int[parameters.length];

        for (int i = 0; i < parameters.length; i++) {
            this.parameters[i] = parameters[i].getBytes();
        }
    }

    public String[] get(byte[] bytes) {
        String[] res = new String[indexes.length];
        int k = 0;

        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];
            for (int j = 0; j < parameters.length; j++) {
                if (b == parameters[j][indexes[j]]) {
                    indexes[j]++;
                } else {
                    indexes[j] = 0;
                }

                if (indexes[j] == parameters[j].length) {
                    if (bytes[i + 1] == '"') {
                        int p = i + 1;
                        while (bytes[p] != ',' && p < bytes.length - 1) {
                            p++;
                        }

                        res[j] = new String(bytes, i + 3, p - i - 3);
                        indexes[j] = 0;
                        k++;
                        if (k == res.length) {
                            Arrays.fill(indexes, 0);
                            return res;
                        }
                    } else {
                        indexes[j] = 0;
                    }
                }

            }
        }


        Arrays.fill(indexes, 0);
        return res;
    }
}
