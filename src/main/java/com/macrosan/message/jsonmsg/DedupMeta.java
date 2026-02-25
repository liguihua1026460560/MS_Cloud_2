package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Objects;

/**
 * 存储重删引用信息
 */
@Data
@CompiledJson
@Accessors(chain = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DedupMeta {
   @JsonAttribute
   public String storage;

  @JsonAttribute
  public String bucket;

  @JsonAttribute
  public String key;

  @JsonAttribute
  public String versionId;

  @JsonAttribute
  public long count;

  @JsonAttribute
  public String fileName;

  @JsonAttribute
  public String etag;

  @JsonAttribute
  public boolean deleteMark;

  @JsonAttribute
  public String versionNum;

  public static final DedupMeta ERROR_DEDUP_META = new DedupMeta().setEtag("error");
  public static final DedupMeta NOT_FOUND_DEDUP_META = new DedupMeta().setEtag("not found");

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (o instanceof DedupMeta) {
      DedupMeta record = (DedupMeta) o;
      return Objects.equals(etag, record.etag) &&
              Objects.equals(storage, record.storage);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(versionNum, deleteMark, key, bucket, etag, storage);
  }
}
