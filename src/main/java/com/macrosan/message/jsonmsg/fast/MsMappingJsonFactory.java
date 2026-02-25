package com.macrosan.message.jsonmsg.fast;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.json.UTF8JsonGenerator;
import com.fasterxml.jackson.core.json.WriterBasedJsonGenerator;
import com.fasterxml.jackson.databind.MappingJsonFactory;

import java.io.*;

public class MsMappingJsonFactory extends MappingJsonFactory {
    MsMappingJsonFactory() {
        super();
    }

    protected JsonGenerator _createGenerator(Writer out, IOContext ctxt) throws IOException {
        WriterBasedJsonGenerator gen = new MsJsonGenerator.MsWriterBasedJsonGenerator(ctxt,
                _generatorFeatures, _objectCodec, out);
        if (_characterEscapes != null) {
            gen.setCharacterEscapes(_characterEscapes);
        }
        return gen;
    }

    protected JsonParser _createParser(char[] data, int offset, int len, IOContext ctxt,
                                       boolean recyclable) throws IOException {
        return new MsReaderBasedJsonParser(ctxt, _parserFeatures, null, _objectCodec,
                _rootCharSymbols.makeChild(_factoryFeatures),
                data, offset, offset + len, recyclable);
    }

    public JsonParser createParser(String content) throws IOException, JsonParseException {
        final int strLen = content.length();
        if ((_inputDecorator != null) || (strLen > 0x8000) || !canUseCharArrays()) {
            IOContext ctxt = _createContext(content, true);
            char[] buf = new char[strLen];
            content.getChars(0, strLen, buf, 0);
            return _createParser(buf, 0, strLen, ctxt, false);
        }

        IOContext ctxt = _createContext(content, true);
        char[] buf = ctxt.allocTokenBuffer(strLen);
        content.getChars(0, strLen, buf, 0);
        return _createParser(buf, 0, strLen, ctxt, true);
    }

    protected JsonGenerator _createUTF8Generator(OutputStream out, IOContext ctxt) throws IOException {
        UTF8JsonGenerator gen = new MsJsonGenerator.MsUTF8JsonGenerator(ctxt,
                _generatorFeatures, _objectCodec, out);
        if (_characterEscapes != null) {
            gen.setCharacterEscapes(_characterEscapes);
        }
        return gen;
    }
}
