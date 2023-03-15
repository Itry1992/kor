package adapter.test.mock;

import com.tong.kafka.manager.vo.TlqBrokerNode;

import java.util.*;
import java.util.stream.Collectors;

public class MockData {
    public final List<TlqBrokerNode> tlqBrokerNodes;

    public final List<String> topics;

    public MockData() {
        int[] ints = {1000, 1001, 1002};
        tlqBrokerNodes = Arrays.stream(ints).mapToObj(i -> {
            TlqBrokerNode tlqBrokerNode = new TlqBrokerNode();
            tlqBrokerNode.setBrokerId(i);
            tlqBrokerNode.setPort("9999");
            tlqBrokerNode.setAddr("localhost");
            return tlqBrokerNode;
        }).collect(Collectors.toList());
        topics = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            topics.add("topic_test" + i);
        }
    }

    public List<TlqBrokerNode> getBrokers() {
        return tlqBrokerNodes;
    }

    public Map<Integer, TlqBrokerNode> getBindMap() {
        Map<Integer, TlqBrokerNode> integerBrokerNodeMap = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            integerBrokerNodeMap.put(i, tlqBrokerNodes.get(i));
        }
        return integerBrokerNodeMap;
    }
}
