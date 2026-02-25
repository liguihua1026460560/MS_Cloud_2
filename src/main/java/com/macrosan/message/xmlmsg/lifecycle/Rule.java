package com.macrosan.message.xmlmsg.lifecycle;

import javax.xml.bind.annotation.*;
import java.util.List;


@XmlRootElement(name = "Rule")
@XmlAccessorType(XmlAccessType.FIELD)
public class Rule {

    public static final String ENABLED = "Enabled";
    public static final String DISABLED = "Disabled";

    @XmlElement(name = "ID", required = true)
    private String id;

    @XmlElement(name = "Filter", required = false)
    private Filter filter;

    @XmlElement(name = "Prefix", required = false)
    private String prefix;

    @XmlElement(name = "Status", required = true)
    private String status;

    @XmlElementRef()
    private List<Condition> conditionList;

    public Rule() {
    }

    public Rule(String id, Filter filter, String prefix, String status, List<Condition> conditionList) {
        this.id = id;
        this.filter = filter;
        this.prefix = prefix;
        this.status = status;
        this.conditionList = conditionList;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public List<Condition> getConditionList() {
        return conditionList;
    }

    public void setConditionList(List<Condition> conditionList) {
        this.conditionList = conditionList;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public String toString() {
        return "Rule{" +
                "id='" + id + '\'' +
                ", filter=" + filter +
                ", prefix='" + prefix + '\'' +
                ", status='" + status + '\'' +
                ", conditionList=" + conditionList +
                '}';
    }

    public Rule copy() {
        Rule rule = new Rule();
        rule.setId(this.id);
        rule.setConditionList(this.conditionList);
        rule.setFilter(this.filter);
        rule.setPrefix(this.prefix);
        rule.setStatus(this.status);
        return rule;
    }
}
