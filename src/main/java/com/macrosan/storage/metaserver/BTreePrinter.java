package com.macrosan.storage.metaserver;

import lombok.extern.java.Log;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.macrosan.storage.metaserver.ObjectSplitTree.isSeparatorLine;

@Log4j2
public class BTreePrinter {

    private final ObjectSplitTree.Node root;
    private List<ObjectSplitTree.Node> mid = new ArrayList<>();

    public BTreePrinter(ObjectSplitTree.Node root) {
        this.root = root;
    }

    private void midOrder(ObjectSplitTree.Node node){
        if(node == null)return;
        midOrder(node.left);
        mid.add(node);
        midOrder(node.right);
    }

    //将node ,index  ====> 存入map中

    private Map<ObjectSplitTree.Node,Integer> map =new HashMap<>();
    private void init(){
        if(root == null)return;
        midOrder(root);
        int offset = 0;
        for (int i = 0; i < mid.size(); i++) {
            map.put(mid.get(i),i + offset);
            offset += numberLength(mid.get(i));
        }
    }

    private int numberLength(ObjectSplitTree.Node node) {
        if (node == null) return 0;
        String value = node.state == 0 ? node.value : node.value + ":" + node.state + ":" + node.migrate;
        return value.length() / 4;
    }

    private void treePrint(List<ObjectSplitTree.Node> nodes, StringBuilder stringBuilder){
        if(nodes.isEmpty())return;
        // nodes : 同一层节点
        String level = printLevel(nodes);//打印同一层的节点
        stringBuilder.append(level);
        List<ObjectSplitTree.Node> children =  new ArrayList<>();
        //顺序遍历下一层节点;
        for (ObjectSplitTree.Node node : nodes) {
            if(node.left != null)children.add(node.left);
            if(node.right != null) children.add(node.right);
        }
        treePrint(children, stringBuilder);//递归打印下一层节点
    }

    private String printLevel(List<ObjectSplitTree.Node> nodes){
        String VLine = "";
        String dataLine = "";
        String line = "";
        int lastNodeIndex = 0;
        int lastRightIndex = 0;
        //上一个节点，是用来获取上一个数字占用了多少个\t
        ObjectSplitTree.Node lastNode = null;
        for (ObjectSplitTree.Node node : nodes) {
            int x =  map.get(node);
            String addEmpty = getEmpty(x-lastNodeIndex);

//            VLine += addEmpty+"|";//竖线拼接

            //数字拼接,重新计算2个数字的间隔，要减去上一个数字占用的位置；
            String numberEmpty = getEmpty(x-lastNodeIndex-numberLength(lastNode));
            String data;
            if (isSeparatorLine(node) || node.state == 0) {
                data = node.value;
            } else {
                data = node.value + ":" + node.state + ":" + node.migrate;
            }
            dataLine += numberEmpty + data;
//            if(node.red())
//                dataLine+= numberEmpty +"\033[91;1m"+node.value+"\033[0m";//打印红色
//            else
//                dataLine += numberEmpty+node.value;

            ObjectSplitTree.Node left  = node.left;
            ObjectSplitTree.Node right = node.right;
            String leftLine  = null;
            String rightLine = null;
            int leftIndex  = -1;
            int rightIndex = -1;
            if(left  != null){
                leftIndex = map.get(left);
                leftLine = getLineToSon(x - leftIndex);
            }
            if(right != null){
                rightIndex = map.get(right);
                rightLine = getLineToSon(rightIndex - x);
            }
            String curLine  = "┏" + (leftLine == null ? "" :leftLine)  + "┻"+ (rightLine == null ? "" : rightLine) + "┓";
            if(leftLine == null && rightLine == null)curLine="";
            //线段之间的间隔
            int dif = (leftIndex == -1 ? x : leftIndex) - lastRightIndex;
            String difEmpty = getEmpty(dif);
            line += difEmpty + curLine;//拼接线段
            lastRightIndex = rightIndex == -1 ? x : rightIndex;
            lastNode = node;
            lastNodeIndex = x;
        }
        return VLine + dataLine +"\n" + line + "\n";
    }

    private String getEmpty(int x){
        StringBuilder empty = new StringBuilder();
        for (int i = 0; i < x; i++) {
            empty.append("    ");
        }
        return empty.toString();
    }


    //链接子线段的长度
    private String getLineToSon(int end){
        String line = "";
        if(end ==0)return line;
        for (int i = 0; i < end; i++) {
            line+="━━━━";
        }
        return line;
    }

    public String treePrint(){
        init();
        List<ObjectSplitTree.Node> nodes =  new ArrayList<>();
        nodes.add(root);
        StringBuilder stringBuilder = new StringBuilder();
        treePrint(nodes, stringBuilder);
        return stringBuilder.toString();
    }
}
