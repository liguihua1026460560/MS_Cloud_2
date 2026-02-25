package com.macrosan.message.jsonmsg.fast;

import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.json.UTF8JsonGenerator;
import com.fasterxml.jackson.core.json.WriterBasedJsonGenerator;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;

public interface MsJsonGenerator {
    void verify() throws IOException;

    class MsWriterBasedJsonGenerator extends WriterBasedJsonGenerator implements MsJsonGenerator {

        public MsWriterBasedJsonGenerator(IOContext ctxt, int features, ObjectCodec codec, Writer w) {
            super(ctxt, features, codec, w);
        }

        @Override
        public void verify() throws IOException {
            _verifyValueWrite(WRITE_STRING);
        }
    }

    class MsUTF8JsonGenerator extends UTF8JsonGenerator implements MsJsonGenerator {

        public MsUTF8JsonGenerator(IOContext ctxt, int features, ObjectCodec codec, OutputStream w) {
            super(ctxt, features, codec, w);
        }

        @Override
        public void verify() throws IOException {
            _verifyValueWrite(WRITE_STRING);
        }
    }
}
