package com.macrosan.message.xmlmsg.lifecycle;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;


@XmlRootElement(name = "LifecycleConfiguration")
@XmlAccessorType(XmlAccessType.FIELD)
public class LifecycleConfiguration {

    @XmlElement(name = "Rule", required = true)
    private List<Rule> rules;

    public LifecycleConfiguration() {
    }

    public LifecycleConfiguration(List<Rule> rules) {
        this.rules = rules;
    }

    public List<Rule> getRules() {
        return rules;
    }

    public void setRules(List<Rule> rules) {
        this.rules = rules;
    }

    @Override
    public String toString() {
        return "LifecycleConfiguration{" +
                "rules=" + rules +
                '}';
    }
}
