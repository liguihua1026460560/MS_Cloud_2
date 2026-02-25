package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@CompiledJson
@Accessors(chain = true)
public class BucketLogInfo {
    @JsonAttribute
    public String bucketOwner = "-";
    @JsonAttribute
    public String bucket = "-";
    @JsonAttribute
    public String time = "-";
    @JsonAttribute
    public String remoteIp = "-";
    @JsonAttribute
    public String requester = "-";
    @JsonAttribute
    public String requestId = "-";
    @JsonAttribute
    public String operation = "-";
    @JsonAttribute
    public String key = "-";
    @JsonAttribute
    public String requestUri = "-";
    @JsonAttribute
    public String httpStatus = "-";
    @JsonAttribute
    public String errorCode = "-";
    @JsonAttribute
    public String bytesSent = "-";
    @JsonAttribute
    public String objectSize = "-";
    @JsonAttribute
    public String totalTime = "-";
    @JsonAttribute
    public String turnAroundTime = "-";
    @JsonAttribute
    public String referer = "-";
    @JsonAttribute
    public String userAgent = "-";
    @JsonAttribute
    public String httpVersionId = "-";
    @JsonAttribute
    public String versionId = "-";
    @JsonAttribute
    public String hostId = "-";
    @JsonAttribute
    public String singnatureVersion = "-";
    @JsonAttribute
    public String authType = "-";


    @Override
    public String toString(){
        return bucketOwner + " " + bucket + " " +
                time + " " + remoteIp + " " +
                requester + " " + requestId + " " +
                operation + " " + key + " " + versionId + " "+
                requestUri + " " + httpStatus + " " +
                errorCode + " " + bytesSent + " " +
                objectSize + " " + totalTime + " " +
                turnAroundTime + " " + referer + " " +
                userAgent + " " + httpVersionId + " " +
                hostId + " " + singnatureVersion + " " +
                authType;
    }
}
