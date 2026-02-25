package com.macrosan.message.jsonmsg;

import com.alibaba.fastjson.annotation.JSONField;
import com.dslplatform.json.CompiledJson;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@CompiledJson
@Accessors(chain = true)
public class PolicyString {

    @JSONField(name = "prefix")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String prefix;

    @JSONField(name = "moss:prefix")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossPrefix;

    @JSONField(name = "delimiter")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String delimiter;

    @JSONField(name = "moss:delimiter")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossDelimiter;

    @JSONField(name = "locationconstraint")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String locationConstraint;

    @JSONField(name = "moss:locationconstraint")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossLocationConstraint;

    @JSONField(name = "signatureversion")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String signatureVersion;

    @JSONField(name = "moss:signatureversion")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossSignatureVersion;

    @JSONField(name = "versionid")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String versionId;

    @JSONField(name = "moss:versionid")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossVersionId;

    @JSONField(name = "x-amz-acl")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String xAmzAcl;

    @JSONField(name = "moss:x-amz-acl")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossXAmzAcl;

    @JSONField(name = "x-amz-copy-source")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String xAmzCopySource;

    @JSONField(name = "moss:x-amz-copy-source")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossXAmzCopySource;

    @JSONField(name = "x-amz-grant-full-control")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String xAmzGrantFullControl;

    @JSONField(name = "moss:x-amz-grant-full-control")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossXAmzGrantFullControl;

    @JSONField(name = "x-amz-grant-read")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String xAmzGrantRead;

    @JSONField(name = "moss:x-amz-grant-read")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossXAmzGrantRead;

    @JSONField(name = "x-amz-grant-write")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String xAmzGrantWrite;

    @JSONField(name = "moss:x-amz-grant-write")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossXAmzGrantWrite;

    @JSONField(name = "x-amz-grant-read-acp")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String xAmzGrantReadAcp;

    @JSONField(name = "moss:x-amz-grant-read-acp")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossXAmzGrantReadAcp;

    @JSONField(name = "x-amz-grant-write-acp")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String xAmzGrantWriteAcp;

    @JSONField(name = "moss:x-amz-grant-write-acp")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossXAmzGrantWriteAcp;

    @JSONField(name = "x-amz-metadata-directive")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String xAmzMetadataDirective;

    @JSONField(name = "moss:x-amz-metadata-directive")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossXAmzMetadataDirective;

    @JSONField(name = "x-amz-content-sha256")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String xAmzContentSha256;

    @JSONField(name = "moss:x-amz-content-sha256")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossXAmzContentSha256;

    @JSONField(name = "x-amz-server-side-encryption")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String xAmzServerSideEncryption;

    @JSONField(name = "moss:x-amz-server-side-encryption")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossXAmzServerSideEncryption;

    @JSONField(name = "UserAgent")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String userAgent;

    @JSONField(name = "moss:UserAgent")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossUserAgent;

    @JSONField(name = "object-lock-mode")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String objectLockMode;

    @JSONField(name = "moss:object-lock-mode")
    @JsonDeserialize(using = CustomJsonDateDeserializer.class)
    private String mossObjectLockMode;

}
