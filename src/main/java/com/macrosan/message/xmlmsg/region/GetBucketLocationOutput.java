package com.macrosan.message.xmlmsg.region;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlValue;


/**
 * 获取桶的location
 * @author chengyinfeng
 */
@Data
@Accessors(chain = true)
@NoArgsConstructor
@XmlRootElement(name = "LocationConstraint")
public class GetBucketLocationOutput {

    private String locationConstraint;

    public GetBucketLocationOutput(String location){
        this.locationConstraint = location;
    }

    @XmlValue
    public String getLocationConstraint() {
        return locationConstraint;
    }

}
