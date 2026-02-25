package com.macrosan.message.xmlmsg.inventory;

import lombok.Data;

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
 * @className ListInventoryConfigurationsResult
 * @date 2022/6/16 11:05
 */
@Data
@XmlRootElement(name = "ListInventoryConfigurationsResult")
@XmlAccessorType(XmlAccessType.FIELD)
public class ListInventoryConfigurationsResult {

    @XmlElement(name = "ContinuationToken")
    private String continuationToken;

    @XmlElement(name = "InventoryConfiguration")
    private List<InventoryConfiguration> inventoryConfigurations;

    @XmlElement(name = "IsTruncated")
    private boolean isTruncated;

    @XmlElement(name = "NextContinuationToken")
    private String nextContinuation;
}
