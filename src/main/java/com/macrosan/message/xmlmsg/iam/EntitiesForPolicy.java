package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/8/3 19:00
 * @Email :   joshua_zhang_email@163.com
 */

public class EntitiesForPolicy {
    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "PolicyRoles")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "PolicyRoles", propOrder = {
            "members"
    })
    public static class PolicyRoles {
        @XmlElement(name = "member", required = true)
        List<Member> members = new LinkedList<>();

        @Data
        @Accessors(chain = true)
        @XmlRootElement(name = "member")
        @XmlAccessorType(XmlAccessType.FIELD)
        @XmlType(name = "PolicyRoles.member", propOrder = {
                "roleName",
                "roleId"
        })
        public static class Member {
            @XmlElement(name = "RoleName", required = true)
            String roleName;
            @XmlElement(name = "RoleId", required = true)
            String roleId;
        }
    }

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "PolicyGroups")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "PolicyGroups", propOrder = {
            "members"
    })
    public static class PolicyGroups {
        @XmlElement(name = "member", required = true)
        List<Member> members = new LinkedList<>();
        ;

        @Data
        @Accessors(chain = true)
        @XmlRootElement(name = "member")
        @XmlAccessorType(XmlAccessType.FIELD)
        @XmlType(name = "PolicyGroups.member", propOrder = {
                "groupName",
                "groupId"
        })
        public static class Member {
            @XmlElement(name = "GroupName", required = true)
            String groupName;
            @XmlElement(name = "GroupId", required = true)
            String groupId;
        }
    }


    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "PolicyUsers")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "PolicyUsers", propOrder = {
            "members"
    })
    public static class PolicyUsers {
        @XmlElement(name = "member", required = true)
        List<Member> members = new LinkedList<>();

        @Data
        @Accessors(chain = true)
        @XmlRootElement(name = "member")
        @XmlAccessorType(XmlAccessType.FIELD)
        @XmlType(name = "PolicyUsers.member", propOrder = {
                "userName",
                "userId"
        })
        public static class Member {
            @XmlElement(name = "UserName", required = true)
            String userName;
            @XmlElement(name = "UserId", required = true)
            String userId;
        }
    }

}
