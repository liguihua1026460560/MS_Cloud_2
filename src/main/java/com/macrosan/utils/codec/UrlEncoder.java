package com.macrosan.utils.codec;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.BitSet;

/**
 * UrlEncoder
 *
 * @author liyixin
 */
public class UrlEncoder {
    private static BitSet dontNeedEncoding;
    private static final int CASE_DIFF = ('a' - 'A');

    static {

        /* 初始化不需要编码的字符，包括a-z，A-Z，0-9，以及-_./~，
         * 即ALPHA / DIGIT / "-" / "." / "_" / "/" / "~"(以下称为合法字符，Unreserved Characters)
         *
         * 注意：JDK自带的URLEncoder中，合法字符为-_.*,RFC3986中规定的合法字符为-_.~
         *
         * 移除"*"是因为RFC2396已经过期，大部分URL编码处理程序会将"*"进行编码。
         * 加入"/"是因为业务需要
         */

        dontNeedEncoding = new BitSet(256);
        int i;
        for (i = 'a'; i <= 'z'; i++) {
            dontNeedEncoding.set(i);
        }
        for (i = 'A'; i <= 'Z'; i++) {
            dontNeedEncoding.set(i);
        }
        for (i = '0'; i <= '9'; i++) {
            dontNeedEncoding.set(i);
        }
        dontNeedEncoding.set('-');
        dontNeedEncoding.set('_');
        dontNeedEncoding.set('.');
        dontNeedEncoding.set('/');
        dontNeedEncoding.set('~');
    }

    private UrlEncoder() {
    }

    public static String encode(String s, String charset) {
        return encode(s, Charset.forName(charset));
    }

    /**
     * 该方法移除了对Unicode中代理对的处理，相较于JDK自带的url编码有轻微的性能提升
     *
     * @param s       待编码字符串
     * @param charset 字符集
     * @return 编码之后的字符串
     */
    public static String encode(String s, Charset charset) {
        boolean needToChange = false;
        StringBuilder out = new StringBuilder(s.length());
        char[] buf = new char[s.length()];
        int index = 0;

        for (int i = 0; i < s.length(); ) {
            int c = (int) s.charAt(i);
            if (dontNeedEncoding.get(c)) {
                out.append((char) c);
                i++;
            } else {
                //在一次编码中尽量多转换一些字符
                do {
                    buf[index] = (char) c;
                    index++;
                    i++;
                } while (i < s.length() && !dontNeedEncoding.get((c = (int) s.charAt(i))));

                //此处Arrays.copyOf()会产生不必要的垃圾，但是目前没有更好的办法
                byte[] bytes = new String(Arrays.copyOf(buf, index)).getBytes(charset);

                //将每个byte拆成两个char，例如255 --> 0xFF
                for (byte b : bytes) {
                    out.append('%');

                    //由于是十六进制表示，所以字母的范围是a-f
                    char ch = Character.forDigit((b >> 4) & 0xF, 16);
                    if (ch >= 'a' && ch <= 'f') {
                        //大小写转换
                        ch -= CASE_DIFF;
                    }
                    out.append(ch);

                    ch = Character.forDigit(b & 0xF, 16);
                    if (ch >= 'a' && ch <= 'f') {
                        ch -= CASE_DIFF;
                    }
                    out.append(ch);
                }
                index = 0;
                needToChange = true;
            }
        }
        return needToChange ? out.toString() : s;
    }
}
