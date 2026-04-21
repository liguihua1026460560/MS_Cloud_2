package com.macrosan.utils.serialize;

import com.sun.xml.internal.bind.marshaller.CharacterEscapeHandler;

import java.io.IOException;
import java.io.Writer;

public class CustomEscapeHandler implements CharacterEscapeHandler {
    @Override
    public void escape(char[] ch, int start, int length, boolean isAttVal, Writer out) throws IOException {
        for (int i = start; i < start + length; i++) {
            char c = ch[i];
            switch (c) {
                case '"':
                    out.write("&quot;");
                    break;
                case '&':
                    out.write("&amp;");
                    break;
                case '<':
                    out.write("&lt;");
                    break;
                case '>':
                    out.write("&gt;");
                    break;
                default:
                    if (ch[i] > 127) {
                        out.write("&#");
                        out.write(Integer.toString(ch[i]));
                        out.write(59);
                    } else {
                        out.write(ch[i]);
                    }
            }
        }
    }
}
