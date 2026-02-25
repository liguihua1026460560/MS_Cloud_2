package com.macrosan.storage.metaserver;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.functional.Tuple2;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * 桶操作对象时根据索引树找到对应的vnode
 */
@Log4j2
public class ObjectSplitTree {

    private final ReentrantReadWriteLock reentrantLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = reentrantLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = reentrantLock.writeLock();

    private final StoragePool storagePool;
    private final String bucketName;

    public ObjectSplitTree(String bucketName, String index) {
        this.bucketName = bucketName;
        storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        deserialize(index);
    }

    /**
     * 根据对象名查找对象所在的分片
     * @param key 对象名称
     * @return 分片所对应的vnode
     */
    public String find(String key) {
        try {
            readLock.lock();
            Node current = root;
            while (isSeparatorLine(current)) {
                String separator = current.value.substring(1);
                if (key.compareTo(separator) <= 0) {
                    current = current.left;
                } else {
                    current = current.right;
                }
            }
            return current.value;
        } finally {
           readLock.unlock();
        }
    }

    public Tuple2<String, String> findMigrate(String key) {
        try {
            readLock.lock();
            Node current = root;
            while (isSeparatorLine(current)) {
                String separator = current.value.substring(1);
                if (key.compareTo(separator) <= 0) {
                    current = current.left;
                } else {
                    current = current.right;
                }
            }
            return new Tuple2<>(current.value, current.state == 1 ? current.migrate : null);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 获取所有非空叶子节点，即为桶最终分配的分片,并且按照分割线，从小到大排序
     */
    public List<String> getAllLeafNode() {
        try {
            readLock.lock();
            Queue<String> queue = new ArrayDeque<>();
            preErgodic(root, queue);
            List<String> collect = queue.stream()
                    .filter(value -> !EMPTY.equals(value) && value.charAt(0) != SEPARATOR_LINE_PREFIX)
                    .collect(Collectors.toList());
            return collect;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 将当前索引序列化成字符串保存与redis中
     */
    public String serialize() {
        try {
            readLock.lock();
            Queue<String> queue = new ArrayDeque<>();
            preSerialize(root, queue);
            return JSON.toJSONString(queue);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 根据redis中的记录，反序列化为索引
     */
    public void deserialize(String tree) {
        try {
            writeLock.lock();
            if (StringUtils.isBlank(tree)) {
                String bucketVnode = storagePool.getBucketVnodeId(bucketName);
                root = new Node(bucketVnode);
                root.left = root.right = null;
                return;
            }
            Queue<String> queue = Json.decodeValue(tree, new TypeReference<Queue<String>>() {});
            this.root = preDeserialize(queue);
        } finally {
            writeLock.unlock();
        }
    }

    public Node leftNeighbor(Node node) {
        try {
            readLock.lock();
            if (isLeft(node)) {
                Node parent = node.parent;
                Node cur = node;
                while (isSeparatorLine(parent) && cur == parent.left) {
                    cur = parent;
                    parent = parent.parent;
                }
                if (parent == null || parent.left == null) {
                    return null;
                }
                return max(parent.left);
            }

            if (isRight(node)) {
                Node parent = node.parent;
                return max(parent.left);
            }
            return null;
        } finally {
            readLock.unlock();
        }
    }

    public Node rightNeighbor(Node node) {
        try {
            readLock.lock();
            if (isLeft(node)) {
                Node parent = node.parent;
                return min(parent.right);
            }
            if (isRight(node)) {
                Node parent = node.parent;
                Node cur = node;
                while (isSeparatorLine(parent) && cur == parent.right) {
                    cur = parent;
                    parent = parent.parent;
                }
                if (parent == null || parent.right == null) {
                    return null;
                }
                return min(parent.right);
            }
            return null;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 查看节点在哪两个分割线之间
     * @param vnode 节点中的vnode
     * @return 上下界分割线
     */
    public Tuple2<String, String> queryNodeUpperBoundAndLowerBound(String vnode) {
        Tuple2<String, String> res = null;
        try {
            readLock.lock();
            Node node = findNodeByValue(vnode);
            if (isLeft(node)) {
                String upperBound = node.parent.value.substring(1);
                String lowerBound = "";
                Node cur = node.parent;
                while (isLeft(cur)) {
                    cur = cur.parent;
                }
                if (isRight(cur)) {
                    lowerBound = cur.parent.value.substring(1);
                }
                res = new Tuple2<>(lowerBound, upperBound);
            } else if (isRight(node)) {
                String lowerBound = node.parent.value.substring(1);
                String upperBound = null;
                Node cur = node.parent;
                while (isRight(cur)) {
                    cur = cur.parent;
                }
                if (isLeft(cur)) {
                    upperBound = cur.parent.value.substring(1);
                }
                res = new Tuple2<>(lowerBound, upperBound);
            }
        } finally {
            readLock.unlock();
        }
        return res;
    }

    /**
     * 当分片达到阈值时，需要开始迁移数据，获取分割线后，拓展该叶子节点
     * @param sourceVnode 需要被扩展的分片
     * @param newVnode 新分配的分片
     * @param newSeparator 新的分割线
     */
    public boolean expansionNode(String sourceVnode, String newVnode, String newSeparator, boolean newNodeIsLeft) {
        try {
            writeLock.lock();
            log.info("bucket {} start expansion node:{} new vnode:{} separator:{}", bucketName, sourceVnode, newVnode, newSeparator);
            print();

            // 判断树结构中是否已经存在newVnode
            Node newNode = find(root, newVnode);
            if (newNode != null) {
                // 判断树结构是否符合拓展之后的规则，若符合则不进行更新
                Node srcNode = find(root, sourceVnode);
                Node separator = find(root, newSeparator);
                if (srcNode != null && separator != null && srcNode.parent == newNode.parent && newNode.parent == separator) {
                    log.info("The bucket {} current tree has met the expansion rules src={} newVnode={} newSeparator={}!", bucketName, sourceVnode, newVnode, newSeparator);
                    return true;
                }
                log.error("bucket {} newVnode:{} is exists!", bucketName, newVnode);
                return false;
            }

            Node node = find(root, sourceVnode, newSeparator);
            if (node == null) {
                log.error("bucket {} expansion node {} error!", bucketName, sourceVnode);
                return false;
            }
            // 重置节点状态
            node.state = 0;
            node.migrate = null;
            node.value = newSeparator;
            // 设置孩子节点
            if (newNodeIsLeft) {
                node.right = generateNode(sourceVnode);
                node.left = generateNode(newVnode);
            } else {
                node.left = generateNode(sourceVnode);
                node.right = generateNode(newVnode);
            }
            // 设置父节点
            node.right.parent = node.left.parent = node;
            log.info("bucket {} expansion node:{} new vnode:{} separator:{} is successfully.", bucketName, sourceVnode, newVnode, newSeparator);
            print();
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 将源节点向目标节点进行合并或者替换
     * @param sourceVnode 源节点Vnode值
     * @param targetVnode 目标节点Vnode值
     * @return 合并结果
     */
    public boolean merge(String sourceVnode, String targetVnode) {
        try {
            writeLock.lock();
            log.info("bucket {} start merge node:{} {}", bucketName, sourceVnode, targetVnode);
            print();
            Node src = findNodeByValue(sourceVnode);
            Node target = findNodeByValue(targetVnode);
            // 合并前，先进行检查
            if (src == null) {
                if (target != null) {
                    log.info("sourceNode:{} and targetNode:{} already merged", sourceVnode, targetVnode);
                    return true;
                }
                log.error("sourceNode:{} is nonexistent", sourceVnode);
                return false;
            }
            Node intersection = intersection(src, target);
            if (intersection != null) {
                intersection.value = targetVnode;
                intersection.left = null;
                intersection.right = null;
            } else {
                src.value = targetVnode;
                src.state = 0;
                src.migrate = null;
            }
            log.info("bucket {} start merge node:{} {} success", bucketName, sourceVnode, targetVnode);
            print();
            return true;
        } catch (Exception e) {
            log.error("merge node {} {} error {}", sourceVnode, targetVnode, e);
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    public boolean updateIntersectionSeparator(String sourceVnode, String destinationVnode, String oldSeparator, String newSeparator) {
        try {
            writeLock.lock();

            log.info("bucket {} start update sourceVnode:{} destVnode:{} the intersection:{} to newSeparator:{}.", bucketName, sourceVnode, destinationVnode, oldSeparator, newSeparator);

            print();

            Node src = find(root, sourceVnode);
            Node dest = find(root, destinationVnode);
            final Node intersection = intersection(src, dest);

            if (intersection == null) {
                log.error("sourceNode:{} and destNode:{} no intersecting vertices!", sourceVnode, destinationVnode);
                return false;
            }

            if (!oldSeparator.equals(intersection.value) && !newSeparator.equals(intersection.value)) {
                log.error("the bucket {} sourceVnode {} destVnode {} intersection {} oldSeparator {} and newSeparator {} is different.",
                        bucketName, sourceVnode, destinationVnode, intersection.value, oldSeparator, newSeparator);
                return false;
            }

            intersection.value = newSeparator;
            log.info("bucket {} update node old value:{} new value:{} success", bucketName, oldSeparator, newSeparator);
            print();

            return true;
        } catch (Exception e) {
            log.error("update separator error ", e);
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    public boolean updateNodeState(String vnode, int state, String migrateVnode) {
        try {
            writeLock.lock();
            log.info("bucket {} start update node:{} state, new state:{}, migrateVnode:{}", bucketName, vnode, state, migrateVnode);
            print();
            Node node = find(root, vnode);
            if (node == null) {
                log.info("update node {} state error!", vnode);
                return false;
            }
            node.state = state;
            node.migrate = migrateVnode;
            log.info("bucket {} start update node:{} state, new state:{}, migrateVnode:{} success", bucketName, vnode, state, migrateVnode);
            print();
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    public Node findNodeByValue(String vnode) {
        try {
            readLock.lock();
            return find(root, vnode);
        } finally {
            readLock.unlock();
        }
    }

    public static final Character SEPARATOR_LINE_PREFIX = '*';
    private static final String EMPTY = "#";
    private Node root;

    public static class Node {
        public String value;
        public Node left;
        public Node right;
        public Node parent;
        /**
         * 记录节点状态,0表示正常,1表示节点正在迁移
         */
        public int state;
        public String migrate;
        public Node(String value) {
            this.value = value;
        }
        public boolean red() {
            return !isSeparatorLine(this);
        }

        @Override
        public String toString() {
            return "Node{" +
                    "value='" + value + '\'' +
                    ", left=" + left +
                    ", right=" + right +
                    ", state=" + state +
                    ", migrate='" + migrate + '\'' +
                    '}';
        }
    }

    private Node find(Node head, String vnode) {
        Queue<Node> nodes = layerErgodic(head);
        for (Node node : nodes) {
            if (node.value.equals(vnode)) {
                return node;
            }
        }
        return null;
    }

    private Node find(Node head, String vnode, String newSeparator) {
        Node current = head;
        while (isSeparatorLine(current)) {
            String separator = current.value;
            int compare = newSeparator.compareTo(separator);
            if (compare <= 0) {
                current = current.left;
            } else {
                current = current.right;
            }
        }
        if (current.value.equals(vnode)) {
            return current;
        }
        return null;
    }

    private void preSerialize(Node head, Queue<String> queue) {
        if (head == null) {
            queue.add(EMPTY);
        } else {
            if (isSeparatorLine(head) || head.state == 0) {
                queue.add(head.value);
            } else {
                queue.add(head.value + ":" + head.state + ":" + head.migrate);
            }
            preSerialize(head.left, queue);
            preSerialize(head.right, queue);
        }
    }

    private void preErgodic(Node head, Queue<String> queue) {
        if (head == null) {
            queue.add(EMPTY);
        } else {
            queue.add(head.value);
            preErgodic(head.left, queue);
            preErgodic(head.right, queue);
        }
    }

    private Node preDeserialize(Queue<String> queue) {
        String value = queue.poll();
        if (value == null || EMPTY.equals(value)) {
            return null;
        }
        Node node;
        if (value.charAt(0) != SEPARATOR_LINE_PREFIX) {
            String[] strings = value.split(":");
            node = new Node(strings[0]);
            if (strings.length == 3) {
                node.state = Integer.parseInt(strings[1]);
                node.migrate = "null".equals(strings[2]) ? null : strings[2];
            }
        } else {
            node = new Node(value);
        }
        node.left = preDeserialize(queue);
        node.right = preDeserialize(queue);
        if (node.left != null) {
            node.left.parent = node;
        }
        if (node.right != null) {
            node.right.parent = node;
        }
        return node;
    }

    private Queue<Node> layerErgodic(Node head) {
        Queue<Node> result = new ArrayDeque<>();
        try {
            if (head == null) {
                return result;
            }
            result.offer(head);
            Queue<Node> queue = new ArrayDeque<>();
            queue.offer(head);
            while (!queue.isEmpty()) {
                Node current = queue.poll();
                if (current.left != null) {
                    result.offer(current.left);
                    queue.offer(current.left);
                }
                if (current.right != null) {
                    result.offer(current.right);
                    queue.offer(current.right);
                }
            }
            return result;
        } catch (Exception e) {
            log.error("layer error", e);
            return result;
        }
    }

    private static Node generateNode(String value) {
        if (value == null || EMPTY.equals(value)) {
            return null;
        }
        return new Node(value);
    }

    private Node min(Node node) {
        Node current = node;
        while (isSeparatorLine(current)) {
            current = current.left;
        }
        return current;
    }

    private Node max(Node node) {
        Node current = node;
        while (isSeparatorLine(current)) {
            current = current.right;
        }
        return current;
    }

    public static Node intersection(Node node1, Node node2) {
        if (node1 == null || node2 == null) {
            return null;
        }
        Node cur1 = node1;
        Node cur2 = node2;
        while (cur1 != cur2) {
            cur1 = cur1 == null ? node2 : cur1.parent;
            cur2 = cur2 == null ? node1 : cur2.parent;
        }
        return cur1;
    }

    public static boolean isLeft(ObjectSplitTree.Node node) {
        if (node == null || !isSeparatorLine(node.parent)) {
            return false;
        }
        return node == node.parent.left;
    }

    public static boolean isRight(ObjectSplitTree.Node node) {
        if (node == null || !isSeparatorLine(node.parent)) {
            return false;
        }
        return node == node.parent.right;
    }

    public static boolean isSeparatorLine(Node node) {
        return node != null && node.value.charAt(0) == SEPARATOR_LINE_PREFIX;
    }

    @Override
    public String toString() {
        return "\n" + new BTreePrinter(root).treePrint();
    }

    public void print() {
        try {
            String s = new BTreePrinter(root).treePrint();
            log.info( bucketName + " Tree morphology as follows" + ":\n" + s);
        } catch (Exception ignored) {}
    }
}