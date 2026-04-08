package io.streamnative.jms;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;

import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.Message;
import javax.jms.Topic;
import java.util.HashMap;
import java.util.Map;

/**
 * Receives text messages from a Pulsar topic via the JMS API.
 *
 * Usage:
 *   java -jar jms-consumer.jar [topic] [numMessages] [timeoutMs]
 *
 * Defaults:
 *   topic       = persistent://public/default/jms-test
 *   numMessages = 5   (0 = receive indefinitely until timeout)
 *   timeoutMs   = 5000
 */
public class JmsConsumer {

    private static final String BROKER_SERVICE_URL =
            "";
    private static final String WEB_SERVICE_URL =
            "";
    private static final String TOKEN =
            "";

    public static void main(String[] args) throws Exception {
        String topicName   = args.length > 0 ? args[0] : "persistent://public/default/output-1";
        int    numMessages = args.length > 1 ? Integer.parseInt(args[1]) : 200000000;

        Map<String, Object> config = new HashMap<>();
        config.put("brokerServiceUrl", BROKER_SERVICE_URL);
        config.put("webServiceUrl", WEB_SERVICE_URL);
        config.put("authPlugin", "org.apache.pulsar.client.impl.auth.AuthenticationToken");
        config.put("authParams", TOKEN);

        System.out.printf("Connecting to broker %s ...%n", BROKER_SERVICE_URL);

        try (PulsarConnectionFactory factory = new PulsarConnectionFactory(config);
             JMSContext context = factory.createContext()) {

            Topic topic = context.createTopic(topicName);

            try (JMSConsumer consumer = context.createConsumer(topic)) {
                System.out.printf("Listening on %s (expecting %s messages) ...%n",
                        topicName, numMessages == 0 ? "unlimited" : numMessages);

                int received = 0;
                while (numMessages == 0 || received < numMessages) {
                    Message message = consumer.receive();
                    if (message == null) {
                        System.out.println("Timed out waiting for message.");
                        break;
                    }

                    String body = message.getBody(String.class);
                    received++;
                    System.out.printf("Received [%d]: %s  (testKey=%s)%n",
                            received, body, message.getObjectProperty("testKey"));
                }

                System.out.printf("Done. Total received: %d%n", received);
            }
        }
    }
}
