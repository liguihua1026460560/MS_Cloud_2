package com.macrosan.component.enums;

/**
 * @author zhaoyang
 * @date 2023/11/22
 **/
public enum ErrorEnum {
    IMAGE_TOTAL_PIXEL_ERROR(10001, "图片处理原图超过总像素的限制", "image"),
    IMAGE_NOT_SUPPORTED(10002, "图片格式不支持", "image"),
    IMAGE_SIZE_ERROR(10003, "图片处理原图大小超过最大限制", "image"),
    IMAGE_NOT_FOUND(10004, "图片不存在", "image"),
    IMAGE_CORRUPTION(10005, "图片处理原图损坏", "image"),
    IMAGE_SCALE_TOTAL_PIXEL_ERROR(10006, "图片处理缩放超过目的图片像素值上限", "image"),
    IMAGE_SCALE_SINGLE_SIDED_PIXEL_ERROR(10007, "图片处理缩放超过目的图片单边像素值上限", "image"),

    IMAGE_PIXEL_ERROR(10008, "图片处理原图宽或高超过单边30000px的限制", "image"),

    CROP_ORIGIN_EXCEEDS_IMAGE_BOUNDARY(10009, "裁剪原点超出图片边界", "image"),

    WATERMARK_IMAGE_NOT_FOUND(10010,"图片处理水印操作水印Logo图不存在","image"),

    ROTATE_IMAGE_PIXEL_TOO_LARGE(10011,"图片处理原图长边超过支持旋转的单边上限","image"),
    WATERMARK_IMAGE_FORMAT_NOT_SUPPORTED(10012,"图片处理水印Logo图为不支持的格式","image"),
    WATERMARK_IMAGE_SIZE_TOO_LARGE(10013,"图片处理水印Logo图大小超过限制","image"),
    WATERMARK_IMAGE_TOTAL_PIXEL_ERROR(10014,"图片处理水印Logo总像素超过限制","image"),
    WATERMARK_IMAGE_PIXEL_ERROR(10015,"图片处理水印Logo单边像素超过限制","image"),

    VIDEO_FORMAT_NOT_SUPPORTED(20001, "视频格式不支持", "video"),
    VIDEO_CORRUPTION(20002, "视频损坏", "video"),
    START_TIME_EXCEEDS_VIDEO_DURATION(20003, "起始时间超过视频总时长", "video"),
    VIDEO_NOT_FOUND(20004, "视频不存在", "video"),
    VIDEO_CODEC_NOT_SUPPORTED(20005, "视频编码格式不支持", "video"),


    UNKNOWN_ERROR(10000, "未知错误", "video");

    private int errorCode;
    private String errorMessage;

    private String processType;

    ErrorEnum(int errorCode, String errorMessage, String processType) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.processType = processType;
    }

    public static ErrorEnum getErrorEnum(int errorCode) {
        for (ErrorEnum errorEnum : ErrorEnum.values()) {
            if (errorEnum.errorCode == errorCode) {
                return errorEnum;
            }
        }
        return null;
    }


    public String getErrorMessage() {
        return errorMessage;
    }

    public String getErrorCode() {
        return String.valueOf(errorCode);
    }

    public String getProcessType() {
        return processType;
    }
}
