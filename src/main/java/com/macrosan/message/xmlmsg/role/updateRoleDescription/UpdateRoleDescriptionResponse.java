package com.macrosan.message.xmlmsg.role.updateRoleDescription;

import com.macrosan.message.xmlmsg.iam.ResponseMetadata;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "UpdateRoleDescriptionResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "UpdateRoleDescriptionResponse", propOrder = {
        "updateRoleDescriptionResult",
        "responseMetadata"
})
public class UpdateRoleDescriptionResponse {
    @XmlElement(name = "UpdateRoleDescriptionResult", required = true)
    public UpdateRoleDescriptionResult updateRoleDescriptionResult;
    @XmlElement(name = "ResponseMetadata", required = true)
    public ResponseMetadata responseMetadata;
}
