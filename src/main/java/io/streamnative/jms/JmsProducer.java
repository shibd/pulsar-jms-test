package io.streamnative.jms;

import com.datastax.oss.pulsar.jms.PulsarConnectionFactory;

import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Topic;
import java.util.Map;

/**
 * Sends text messages to a Pulsar topic via the JMS API.
 *
 * Usage:
 *   java -jar jms-producer.jar [brokerServiceUrl] [webServiceUrl] [topic] [numMessages]
 *
 * Defaults (no args needed when running against a local standalone cluster):
 *   brokerServiceUrl = pulsar://127.0.0.1:6650
 *   webServiceUrl    = http://127.0.0.1:8080
 *   topic            = persistent://public/default/jms-test
 *   numMessages      = 5
 */
public class JmsProducer {

    public static void main(String[] args) throws Exception {
        String brokerServiceUrl = args.length > 0 ? args[0] : "pulsar://127.0.0.1:6650";
        String webServiceUrl    = args.length > 1 ? args[1] : "http://127.0.0.1:8080";
        String topicName        = args.length > 2 ? args[2] : "persistent://public/default/jms-test";
        int    numMessages      = args.length > 3 ? Integer.parseInt(args[3]) : 5;

        Map<String, Object> config = Map.of(
                "brokerServiceUrl", brokerServiceUrl,
                "webServiceUrl", webServiceUrl
        );

        System.out.printf("Connecting to broker %s ...%n", brokerServiceUrl);

        try (PulsarConnectionFactory factory = new PulsarConnectionFactory(config);
             JMSContext context = factory.createContext()) {

            Topic topic = context.createTopic(topicName);
            JMSProducer producer = context.createProducer();

            for (int i = 1; i <= numMessages; i++) {
                String body = "hello-jms-" + i;

                producer.setProperty("messageIndex", i);
                producer.setProperty("source", "jms-producer");
                producer.send(topic, body);

                System.out.printf("Sent [%d/%d]: %s%n", i, numMessages, body);
            }

            System.out.println("Done.");
        }
    }
}
