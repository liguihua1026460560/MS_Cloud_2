package com.macrosan.message.xmlmsg.inventory;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * <p></p>
 *
 * @author Administrator
 * @version 1.0
 * @className Schedule
 * @date 2022/6/15 14:01
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "Schedule")
@XmlAccessorType(XmlAccessType.FIELD)
public class Schedule {

    @XmlElement(name = "Frequency")
    public String frequency;

}
