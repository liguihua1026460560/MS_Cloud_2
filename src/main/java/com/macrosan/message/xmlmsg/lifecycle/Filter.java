package com.macrosan.message.xmlmsg.lifecycle;

import javax.xml.bind.annotation.*;
import java.util.List;


@XmlRootElement(name = "Filter")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "LifecycleFilter", propOrder = {
        "prefix",
        "tags"
})
public class Filter {
    @XmlElement(name = "Prefix", required = false)
    private String prefix;

    @XmlElement(name = "Tag", required = false)
    private List<Tag> tags;

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public List<Tag> getTags() {
        return tags;
    }

    public void setTags(List<Tag> tags) {
        this.tags = tags;
    }

    public Filter() {
    }

    public Filter(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public String toString() {
        return "Filter{" +
                "prefix='" + prefix + '\'' +
                ", tags=" + tags +
                '}';
    }
}
