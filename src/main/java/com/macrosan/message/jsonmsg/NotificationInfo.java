package com.macrosan.message.jsonmsg;

import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.xmlmsg.section.ObjectsList;
import lombok.Data;

import java.util.List;

@Data
public class NotificationInfo {
    List<ObjectsList> deletedList;
    String prefix;
    String suffix;
    String[] event;
    MsHttpRequest msHttpRequest;

    public void setInfo(String prefix,String suffix,String[] event,List<ObjectsList> deletedList,MsHttpRequest msHttpRequest){
        this.prefix = prefix;
        this.suffix = suffix;
        this.event = event;
        this.deletedList = deletedList;
        this.msHttpRequest = msHttpRequest;
    }
}
