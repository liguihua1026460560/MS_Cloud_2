package com.macrosan.message.jsonmsg.fast;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.json.ReaderBasedJsonParser;
import com.fasterxml.jackson.core.sym.CharsToNameCanonicalizer;

import java.io.IOException;
import java.io.Reader;

public class MsReaderBasedJsonParser extends ReaderBasedJsonParser {
    int msState = -1;

    public MsReaderBasedJsonParser(IOContext ctxt, int features, Reader r, ObjectCodec codec, CharsToNameCanonicalizer st, char[] inputBuffer, int start, int end, boolean bufferRecyclable) {
        super(ctxt, features, r, codec, st, inputBuffer, start, end, bufferRecyclable);
    }

    public MsReaderBasedJsonParser(IOContext ctxt, int features, Reader r, ObjectCodec codec, CharsToNameCanonicalizer st) {
        super(ctxt, features, r, codec, st);
    }

    public String getText0() throws IOException {
        JsonToken t = _currToken;
        if (t == JsonToken.VALUE_STRING) {
            if (_tokenIncomplete) {
                _tokenIncomplete = false;

                switch (msState) {
                    case 0:
                        msState = -1;
                        _finishSimpleString();
                        break;
                    case 1:
                        msState = -1;
                        _finishMsString();
                        break;
                    case 2:
                        msState = -1;
                        _finishMsString2();
                        break;
                    default:
                        super._finishString();
                }
            }
            return _textBuffer.contentsAsString();
        }
        return _getText2(t);
    }

    protected JsonToken _handleOddValue(int i) throws IOException {
        switch (i) {
            case '\'':
                _tokenIncomplete = true;
                msState = 0;
                return JsonToken.VALUE_STRING;
            case '!':
                _tokenIncomplete = true;
                msState = 1;
                return JsonToken.VALUE_STRING;
            case '@':
                _tokenIncomplete = true;
                msState = 2;
                return JsonToken.VALUE_STRING;
        }

        return super._handleOddValue(i);
    }

    private void _finishMsString() throws IOException {
        int ptr = _inputPtr;

        int len = ((_inputBuffer[ptr] & 0x7FFF) << 16) | _inputBuffer[ptr + 1];
        _textBuffer.resetWithCopy(_inputBuffer, ptr + 2, len);
        _inputPtr = ptr + len + 2;
    }

    private void _finishMsString2() throws IOException {
        int ptr = _inputPtr;

        int len = ((_inputBuffer[ptr] & 0x7FFF) << 15) | _inputBuffer[ptr + 1];
        _textBuffer.resetWithCopy(_inputBuffer, ptr + 2, len);
        _inputPtr = ptr + len + 2;
    }

    private void _finishSimpleString() throws IOException {
        int ptr = _inputPtr;
        final int inputLen = _inputEnd;

        if (ptr < inputLen) {
            do {
                int ch = _inputBuffer[ptr++];

                if (ch == '\'') {
                    _textBuffer.resetWithShared(_inputBuffer, _inputPtr, (ptr - _inputPtr - 1));
                    _inputPtr = ptr;
                    // Yes, we got it all
                    return;
                }
            } while (ptr < inputLen);
        }

        // Either ran out of input, or bumped into an escape sequence...
        _textBuffer.resetWithCopy(_inputBuffer, _inputPtr, (ptr - _inputPtr));
        _inputPtr = ptr;
    }
}
