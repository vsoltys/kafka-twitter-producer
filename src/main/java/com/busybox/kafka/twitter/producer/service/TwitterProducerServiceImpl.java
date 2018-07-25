package com.busybox.kafka.twitter.producer.service;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Service
public class TwitterProducerServiceImpl implements TwitterProducerService {

    private static final String TOPIC_NAME = "api-twitter-topic";
    private static final int MESSAGE_LIMIT = 1000;

    private static final String TERM_TWITTER = "#twitter";
    private static final List<String> TERMS = Lists.newArrayList(TERM_TWITTER);

    @Value("${kafka.producer.client.id}")
    private String producerClientId;

    @Value("${kafka.bootstrap.servers}")
    private String brokerBootstrapServers;

    @Value("${twitter.api.consumer.key}")
    private String consumerKey;

    @Value("${twitter.api.consumer.secret}")
    private String consumerSecret;

    @Value("${twitter.api.access.token}")
    private String accessToken;

    @Value("${twitter.api.access.secret}")
    private String accessSecret;

    @Value("${twitter.api.default.capacity}")
    private int twitterApiDefaultCapacity;

    @Override
    public void run() {
        final Producer<String, String> producer = new KafkaProducer<>(getProperties());

        final BlockingQueue<String> queue = new LinkedBlockingQueue<String>(twitterApiDefaultCapacity);
        final StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        // add some track terms
        endpoint.trackTerms(TERMS);

        final Authentication auth = new OAuth1(consumerKey, consumerSecret, accessToken, accessSecret);

        final Client client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        client.connect();

        for (int msgRead = 0; msgRead < MESSAGE_LIMIT; msgRead++) {
            try {
                final String payload = queue.take();
                System.out.println(payload);
                producer.send(new ProducerRecord<>(TOPIC_NAME, UUID.randomUUID().toString(), payload));
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }

        client.stop();
    }

    private Properties getProperties() {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, producerClientId);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerBootstrapServers);
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());

        return properties;
    }
}
