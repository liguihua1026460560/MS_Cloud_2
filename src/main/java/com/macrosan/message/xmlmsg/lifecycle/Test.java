package com.macrosan.message.xmlmsg.lifecycle;

import com.macrosan.utils.serialize.JaxbUtils;

import java.util.Date;
import java.util.LinkedList;

public class Test {
    public static void main(String[] args) {
        Expiration expiration = new Expiration();
        expiration.setDate(new Date().toString());
        expiration.setDays(10);

        Transition transition = new Transition();
        transition.setDate(new Date().toString());
        transition.setDays(5);

        NoncurrentVersionExpiration noncurrentVersionExpiration = new NoncurrentVersionExpiration();
        noncurrentVersionExpiration.setNoncurrentDays(20);

        NoncurrentVersionTransition noncurrentVersionTransition = new NoncurrentVersionTransition();
        noncurrentVersionTransition.setNoncurrentDays(15);
        noncurrentVersionTransition.setStorageClass("StorageClass");

        Rule rule1 = new Rule("this is id", null, "moss", "enabled", new LinkedList<>());
        rule1.getConditionList().add(transition);
        rule1.getConditionList().add(expiration);
        rule1.getConditionList().add(noncurrentVersionExpiration);
        rule1.getConditionList().add(noncurrentVersionTransition);

        Rule rule2 = new Rule("this is id2", new Filter("this is prefix2"), null, "disabled", new LinkedList<>());
        rule2.getConditionList().add(expiration);
        rule2.getConditionList().add(transition);
        rule2.getConditionList().add(noncurrentVersionTransition);
        rule2.getConditionList().add(noncurrentVersionExpiration);

        LifecycleConfiguration lifecycleConfiguration = new LifecycleConfiguration(new LinkedList<>());
        lifecycleConfiguration.getRules().add(rule1);
        lifecycleConfiguration.getRules().add(rule2);

        /* 初始化Jaxb */
        JaxbUtils.initJaxb();

        String string = new String(JaxbUtils.toByteArray(lifecycleConfiguration));
        System.out.println(string);
        LifecycleConfiguration lifecycle = (LifecycleConfiguration) JaxbUtils.toObject(string);
        System.out.println(lifecycle);

    }
}
