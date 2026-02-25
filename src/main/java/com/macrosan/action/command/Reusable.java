package com.macrosan.action.command;

import com.macrosan.message.jsonmsg.CliResponse;

/**
 * Reusable
 *
 * @author liyixin
 * @date 2019/10/17
 */
public class Reusable {

    private CliResponse response = new CliResponse();

    private static final String[] EMPTY = new String[0];

    private static final String NULL_COMMAND = "";

    CliResponse reset() {
        response.setParams(EMPTY);
        response.setStatus(NULL_COMMAND);
        return response;
    }

    CliResponse response() {
        return reset();
    }

}
