package com.macrosan.message.xmlmsg.tagging;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

@Data
@XmlAccessorType(XmlAccessType.FIELD)
@AllArgsConstructor
@NoArgsConstructor
@XmlType(name = "TagSet", propOrder = {
    "tags"
})
public class TagSet {
    @XmlElement(name = "Tag", required = true)
    private List<Tag> tags;
}
