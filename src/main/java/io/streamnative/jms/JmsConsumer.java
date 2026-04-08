package io.streamnative.jms;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;

import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.Message;
import javax.jms.Topic;
import java.util.Map;

/**
 * Receives text messages from a Pulsar topic via the JMS API.
 *
 * Usage:
 *   java -jar jms-consumer.jar [brokerServiceUrl] [webServiceUrl] [topic] [numMessages] [timeoutMs]
 *
 * Defaults (no args needed when running against a local standalone cluster):
 *   brokerServiceUrl = pulsar://127.0.0.1:6650
 *   webServiceUrl    = http://127.0.0.1:8080
 *   topic            = persistent://public/default/jms-test
 *   numMessages      = 5   (0 = receive indefinitely until timeout)
 *   timeoutMs        = 5000
 */
public class JmsConsumer {

    public static void main(String[] args) throws Exception {
        String brokerServiceUrl = args.length > 0 ? args[0] : "pulsar://127.0.0.1:6650";
        String webServiceUrl    = args.length > 1 ? args[1] : "http://127.0.0.1:8080";
        String topicName        = args.length > 2 ? args[2] : "persistent://public/default/jms-test";
        int    numMessages      = args.length > 3 ? Integer.parseInt(args[3]) : 5;
        long   timeoutMs        = args.length > 4 ? Long.parseLong(args[4]) : 5000L;

        Map<String, Object> config = Map.of(
                "brokerServiceUrl", brokerServiceUrl,
                "webServiceUrl", webServiceUrl
        );

        System.out.printf("Connecting to broker %s ...%n", brokerServiceUrl);

        try (PulsarConnectionFactory factory = new PulsarConnectionFactory(config);
             JMSContext context = factory.createContext()) {

            Topic topic = context.createTopic(topicName);

            try (JMSConsumer consumer = context.createConsumer(topic)) {
                System.out.printf("Listening on %s (expecting %s messages, timeout %dms) ...%n",
                        topicName, numMessages == 0 ? "unlimited" : numMessages, timeoutMs);

                int received = 0;
                while (numMessages == 0 || received < numMessages) {
                    Message message = consumer.receive(timeoutMs);
                    if (message == null) {
                        System.out.println("Timed out waiting for message.");
                        break;
                    }

                    String body = message.getBody(String.class);
                    received++;
                    System.out.printf("Received [%d]: %s  (messageIndex=%s)%n",
                            received, body, message.getObjectProperty("messageIndex"));
                }

                System.out.printf("Done. Total received: %d%n", received);
            }
        }
    }
}
