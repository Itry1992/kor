package com.tong.kafka.tlq.recode.protocol;

import com.tongtech.client.remoting.protocol.RemotingCommand;
import com.tongtech.protobuf.MessageLite;

public class ReRemotingCommand extends RemotingCommand {

    private MessageLite messageLite;

    public MessageLite getMessageLite() {
        return messageLite;
    }

    public void setMessageLite(MessageLite messageLite) {
        this.messageLite = messageLite;
    }

}
