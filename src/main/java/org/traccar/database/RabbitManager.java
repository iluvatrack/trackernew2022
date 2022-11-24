package org.traccar.database;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class RabbitManager {
    private final Config config;

    private Connection connection;
    private Channel channel;

    private static final String QUEUE_NAME = "trackpro";

    private static final String KEY_POSITION = "position";
    private static final String KEY_DEVICE = "device";

    @Inject
    private final ObjectMapper objectMapper;
    @Inject
    private final CacheManager cacheManager;

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitManager.class);

    public RabbitManager(Config config, ObjectMapper objectMapper, CacheManager cacheManager) throws IOException {
        this.config = config;
        this.objectMapper = objectMapper;
        this.cacheManager = cacheManager;

        initConnection();
    }

    public void initConnection() {
        ConnectionFactory factory = new ConnectionFactory();
        String url = config.getString(Keys.RABBIT_URL);

        try {
            factory.setUri(url);
            LOGGER.info(url);
        } catch (URISyntaxException | NoSuchAlgorithmException | KeyManagementException e) {
            e.printStackTrace();
        }

        try {
            connection = factory.newConnection();
            LOGGER.info(String.valueOf(connection));
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
        try {
            channel = connection.createChannel();
            LOGGER.info(String.valueOf(channel));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void publish(Position position) {
        try {
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

        Map<String, Object> data = new HashMap<>();
        Device device = cacheManager.getObject(Device.class, position.getDeviceId());

        data.put(KEY_POSITION, position);

        if (device != null) {
            data.put(KEY_DEVICE, device);
        }

        String json = null;
        try {
            json = objectMapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        try {
            assert json != null;
            channel.basicPublish("", QUEUE_NAME, new AMQP.BasicProperties.Builder()
                    .deliveryMode(2).build(), json.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOGGER.debug(json);
    }
}