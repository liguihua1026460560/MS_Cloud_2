package com.macrosan.message.xmlmsg.inventory;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * <p></p>
 *
 * @author Administrator
 * @version 1.0
 * @className InventoryConfiguration
 * @date 2022/6/15 11:42
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "InventoryConfiguration")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "InventoryConfiguration", propOrder = {

})
public class InventoryConfiguration {

    @XmlElement(name = "Destination")
    private Destination destination;

    @XmlElement(name = "IsEnabled")
    private String isEnabled;

    @XmlElement(name = "Filter")
    private Filter filter;

    @XmlElement(name = "Id")
    private String id;

    @XmlElement(name = "IncludedObjectVersions")
    private String includedObjectVersions;

    @XmlElement(name = "OptionalFields")
    private OptionalFields optionalFields;

    @XmlElement(name = "Schedule")
    private Schedule schedule;

    @XmlElement(name = "InventoryType")
    private String inventoryType;
}
