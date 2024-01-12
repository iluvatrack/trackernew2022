package org.traccar.handler;

import io.netty.channel.ChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.BaseDataHandler;
import org.traccar.database.RabbitManager;
import org.traccar.model.Position;

import jakarta.inject.Inject;

@ChannelHandler.Sharable
public class RabbitDataHandler extends BaseDataHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitDataHandler.class);

    private final RabbitManager rabbitManager;

    @Inject
    public RabbitDataHandler(RabbitManager rabbitManager) {
//        LOGGER.info("RABBIT DATA HANDLER");
        this.rabbitManager = rabbitManager;
    }

    @Override
    protected Position handlePosition(Position position) {

        rabbitManager.publish(position);
        LOGGER.info("Publish Position To Rabbit");

        return position;
    }
}
