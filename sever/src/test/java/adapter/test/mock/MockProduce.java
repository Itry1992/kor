package adapter.test.mock;

import com.tong.kafka.common.TopicPartition;
import com.tong.kafka.common.record.Record;
import com.tong.kafka.exception.MessageTooLagerException;
import com.tong.kafka.manager.vo.TlqBrokerNode;
import com.tong.kafka.produce.AbsTlqProduce;
import com.tong.kafka.produce.vo.KafkaRecordAttr;
import com.tong.kafka.produce.vo.SendResult;
import com.tongtech.client.message.Message;
import com.tongtech.client.message.MessageExt;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MockProduce extends AbsTlqProduce {
    public MockProduce() {
        super(new MockManager());
    }

    private volatile List<Message> cacheMessage;

    private AtomicLong offsetCache = new AtomicLong();

    @Override
    public CompletableFuture<SendResult> sendBatch(TopicPartition tp, List<Record> records, KafkaRecordAttr messageAttr, int timeout) throws MessageTooLagerException {
        List<Message> messages = buildBatchRecord(records, messageAttr, tp);
        Optional<TlqBrokerNode> tlqBroker = manager.getTlqBrokerNode(tp);
        //
        cacheMessage = messages;
        System.out.println("accpet new message from " + tp + "size is " + messages.size());
        offsetCache.addAndGet(messages.size());
        CompletableFuture<SendResult> sendResultCompletableFuture = new CompletableFuture<>();
        SendResult value = new SendResult();
        value.setOffset(offsetCache.get());
        value.setLogAppendTime(System.currentTimeMillis());
        sendResultCompletableFuture.complete(value);
        return sendResultCompletableFuture;
    }


    public List<MessageExt> getCacheMessage() {
        if (cacheMessage == null || cacheMessage.isEmpty())
            return Collections.emptyList();
        long baseOffset = offsetCache.get() - cacheMessage.size();
        List<MessageExt> messageExts = IntStream.range(0, cacheMessage.size()).mapToObj(i -> {
            Message message = cacheMessage.get(i);
            MessageExt messageExt = new MessageExt();
            messageExt.setBody(message.getBody());
            messageExt.setAttr(message.getAttr());
            messageExt.setCommitLogOffset(baseOffset + i);
            messageExt.setTime((int) System.currentTimeMillis());
            return messageExt;
        }).collect(Collectors.toList());
        return messageExts;
    }
}
