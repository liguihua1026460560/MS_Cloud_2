package com.macrosan.utils.functional;

import com.macrosan.constants.ErrorNo;

import java.util.function.Consumer;

/**
 * Result
 * 提供对结果处理的抽象
 *
 * @author liyixin
 * @date 2018/11/17
 */
public interface Result<T> {

    /**
     * 给结果绑定动作
     *
     * @param success 成功时执行的动作
     * @param failure 失败时执行的动作
     */
    void bind(Consumer<T> success, Consumer<Integer> failure);

    /**
     * 判断是否成功
     *
     * @return 成功或失败
     */
    Boolean isSuccess();

    /**
     * 获得成功时的结果，失败时调用返回null
     *
     * @return 成功时的结果
     */
    T getValue();

    /**
     * 获得失败时的错误码，成功时调用返回 {@code ErrorNo.SUCCESS_STATUS}
     *
     * @return 错误码
     */
    int getCode();

    /**
     * 生成一个代表失败的对象的静态方法
     *
     * @param code 错误码
     * @param <T>  成功时返回值类型
     * @return 代表失败的结果
     */
    static <T> Result<T> failure(int code) {
        return new Failure<>(code);
    }

    /**
     * 生成一个代表成功的对象的静态方法
     *
     * @param value 成功时的返回值
     * @param <T>   成功时返回值类型
     * @return 代表成功的结果
     */
    static <T> Result<T> success(T value) {
        return new Success<>(value);
    }

    class Success<T> implements Result<T> {

        private final T value;

        private Success(T t) {
            value = t;
        }

        @Override
        public void bind(Consumer<T> success, Consumer<Integer> failure) {
            success.accept(value);
        }

        @Override
        public Boolean isSuccess() {
            return true;
        }

        @Override
        public T getValue() {
            return value;
        }

        @Override
        public int getCode() {
            return ErrorNo.SUCCESS_STATUS;
        }
    }

    class Failure<T> implements Result<T> {

        private final int code;

        private Failure(int n) {
            code = n;
        }

        @Override
        public void bind(Consumer<T> success, Consumer<Integer> failure) {
            failure.accept(code);
        }

        @Override
        public Boolean isSuccess() {
            return false;
        }

        @Override
        public int getCode() {
            return code;
        }

        @Override
        public T getValue() {
            return null;
        }
    }
}
