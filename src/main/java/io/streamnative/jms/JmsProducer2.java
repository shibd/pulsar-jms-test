package io.streamnative.jms;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;
import java.util.HashMap;
import java.util.Map;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Topic;

/**
 * Sends text messages to a Pulsar topic via the JMS API.
 *
 * Usage:
 *   java -jar jms-producer.jar [topic] [numMessages]
 *
 * Defaults:
 *   topic       = persistent://public/default/jms-test
 *   numMessages = 5
 */
public class JmsProducer2 {

    private static final String BROKER_SERVICE_URL =
            "";
    private static final String WEB_SERVICE_URL =
            "";
    private static final String TOKEN =
            "";

    public static void main(String[] args) throws Exception {
        String topicName   = args.length > 0 ? args[0] : "persistent://public/default/input-2";
        int    numMessages = args.length > 1 ? Integer.parseInt(args[1]) : 100000000;

        Map<String, Object> config = new HashMap<>();
        config.put("brokerServiceUrl", BROKER_SERVICE_URL);
        config.put("webServiceUrl", WEB_SERVICE_URL);
        config.put("authPlugin", "org.apache.pulsar.client.impl.auth.AuthenticationToken");
        config.put("authParams", TOKEN);

        System.out.printf("Connecting to broker %s ...%n", BROKER_SERVICE_URL);

        try (PulsarConnectionFactory factory = new PulsarConnectionFactory(config);
             JMSContext context = factory.createContext()) {

            Topic topic = context.createTopic(topicName);
            JMSProducer producer = context.createProducer();

            for (int i = 1; i <= numMessages; i++) {
                String body = "hello-jms-test2-" + i;
                if (i % 2 == 0) {
                    producer.setProperty("testKey", "abc");
                } else {
                    producer.setProperty("testKey", "efg");
                }

                producer.setProperty("messageIndex", i);
                producer.setProperty("source", "jms-producer");
                producer.send(topic, body);

                System.out.printf("Sent [%d/%d]: %s%n", i, numMessages, body);
                Thread.sleep(1000);
            }

            System.out.println("Done.");
        }
    }
}
