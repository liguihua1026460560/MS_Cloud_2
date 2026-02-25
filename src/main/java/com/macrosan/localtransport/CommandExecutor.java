package com.macrosan.localtransport;

import com.macrosan.message.jsonmsg.CliCommand;
import com.macrosan.message.jsonmsg.CliResponse;
import com.macrosan.utils.cache.ClassUtils;
import com.macrosan.utils.serialize.JsonUtils;
import lombok.extern.log4j.Log4j2;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import reactor.core.publisher.Flux;

import java.util.function.Function;

/**
 * CommandExecutor
 * <p>
 * 用于根据command帧执行对应的command
 *
 * @author liyixin
 * @date 2019/10/17
 */
@Log4j2
public class CommandExecutor {

    private static final IntObjectHashMap<Function<String[], CliResponse>> COMMAND_MAP = new IntObjectHashMap<>();

    private static final String PACKAGE = "com.macrosan.action.command";

    private static final String END_STR = "Command.class";

    private static final String COMMAND_NOT_FOUND = "";

    static {
        init();
    }

    public static void init() {
        ClassUtils.getClassFlux(PACKAGE, END_STR)
                .flatMap(cls -> Flux
                        .fromArray(cls.getDeclaredMethods())
                        .doOnNext(method -> {
                            final Command annotation = method.getAnnotation(Command.class);
                            if (annotation == null) {
                                return;
                            }
                            int key = annotation.value().equals("") ?
                                    method.getName().hashCode() :
                                    annotation.value().hashCode();
                            final Function<String[], CliResponse> function = ClassUtils.generateLambda(cls, method);
                            COMMAND_MAP.put(key, function);
                        }))
                .subscribe();
    }

    public static String execute(CliCommand command) {
        final int key = command.getCommand().hashCode();
        final Function<String[], CliResponse> function = COMMAND_MAP.get(key);
        if (function == null) {
            log.error("can't find command {}", command::getCommand);
            return COMMAND_NOT_FOUND;
        }
        return JsonUtils.toString(function.apply(command.getParams()), CliResponse.class);
    }
}
