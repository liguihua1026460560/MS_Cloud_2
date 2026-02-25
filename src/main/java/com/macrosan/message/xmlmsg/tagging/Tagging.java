package com.macrosan.message.xmlmsg.tagging;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@AllArgsConstructor
@Accessors(chain = true)
@NoArgsConstructor
@XmlRootElement(name = "Tagging")
@XmlAccessorType(XmlAccessType.FIELD)
public class Tagging {
    @XmlElement(name = "TagSet",required = true)
    private TagSet tagSet;
}
