package com.macrosan.message.xmlmsg.inventory;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * <p></p>
 *
 * @author Administrator
 * @version 1.0
 * @className OptionalFields
 * @date 2022/6/15 14:00
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "OptionalFields")
@XmlAccessorType(XmlAccessType.FIELD)
public class OptionalFields {

    @XmlElement(name = "Field", required = true)
    public List<String> fields;

}
