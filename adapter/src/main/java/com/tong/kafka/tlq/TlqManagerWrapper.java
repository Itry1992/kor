package com.tong.kafka.tlq;

import com.tong.kafka.common.AdapterConfig;
import com.tongtech.client.admin.TLQManager;
import com.tongtech.client.exception.TLQClientException;
import com.tongtech.slf4j.Logger;
import com.tongtech.slf4j.LoggerFactory;

import java.util.Optional;

public class TlqManagerWrapper extends AbsTlqWrapper<TLQManager> {
    Logger logger = LoggerFactory.getLogger(TlqManagerWrapper.class);

    public TlqManagerWrapper(AdapterConfig config, String nameSrvAddr, String domainName) {
        super(config);
        TLQManager manager = TlqFactory.createManager(nameSrvAddr, domainName);
        try {
            manager.start();
            t = manager;
            usable = true;
        } catch (TLQClientException e) {
            logger.error("启动htp管理者失败", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    void clear() {
        t.shutdown();
    }

    @Override
    Optional<AbsTlqWrapper<TLQManager>> getNewInstance() {
        TlqManagerWrapper managerWrapper = new TlqManagerWrapper(this.config, this.t.getNamesrvAddr(), this.t.getDomain());
        return Optional.of(managerWrapper);
    }
}
