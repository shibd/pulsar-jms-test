package io.streamnative.jms;


import io.streamnative.pulsar.jms.PulsarConnectionFactory;
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
            "pulsar+ssl://pc-c3720b92.aws-use2-production-snci-pool-kid.streamnative.aws.snio.cloud:6651";
    private static final String WEB_SERVICE_URL =
            "https://pc-c3720b92.aws-use2-production-snci-pool-kid.streamnative.aws.snio.cloud";
    private static final String TOKEN =
            "eyJhbGciOiJSUzI1NiIsImtpZCI6InByb2R1Y3Rpb24ta2V5LTIwMjUtMTItdjEiLCJ0eXAiOiJKV1QifQ.eyJhdWQiOlsidXJuOnNuOmNsb3VkOnNuY2xvdWQiXSwiZXhwIjoxNzc4MjA4OTM5LCJpYXQiOjE3NzU2MTY5NDAsImlzcyI6Imh0dHBzOi8vYXBpa2V5cy5zdHJlYW1uYXRpdmUuY2xvdWQvIiwianRpIjoiMGYxMzQ4YzU5ZGE0NDRkZWExOWM2ODM2OTk4MDM5ZTIiLCJzY29wZSI6W10sInN1YiI6ImFkbWluQHNuY2xvdWQuYXV0aC5zdHJlYW1uYXRpdmUuY2xvdWQifQ.HdnAGo13NF3458uYTMS7Mur807ZYdDoGRCZx6q_gtCnp64ybXGoE12qrsRG3t4l3lQDLoPfrJMD9oGVahs_XpOxRG_wKv2kgQxTkyJuYf3SMk1bpMoP-UJNWIk-FBvPAQT_rc59-uRxOXO9JWM2QyaqR1ze0zUtwOqppvk979rjdcDrchbOaRxh_4F6-9Iy9j4LIwIzsPaJnoWnPNamI3UblfjQpVy-nQYh3PEILicLLoKeegOgSbVpFh6jOUBs2CgDiW0U6wQBgPuW7HlVHtoOZyYoL1XiSV4WtKQ6L6oyLcUtnlIG0pLzAL9u6NO6AaekdiG5Uo5w06QPi-wT7zcbt77vls4nVI1ZGFpIv-zyAUC9XCbPIRKn3TD8o7N_VpbpNG7DVwHKgnT0s1iPEf4zlaDXGF8d9Xf6_K4xFIAk4vKYHqXzhcsc6EyzDLPtUIUQArelqEAO0MIKBSmlJEKus8KEr4umFX5JU7YGSxS0rWCubDO4YdIirZDzuPkuULUbVFa7avFcfHqOmQ3BIYnETeTyUz6WXKDJn88PtXRt4NWCCiIdt82qSbcs8-SlmMVaZYsJ7VtU8soNpCIetGT5FKxDGxNua1LAholmUafTIJjp0XneYiUhGEfc_zA9WPucGMkclg9WtUnPKNW1rKf7XrnAF-iSQTXO3Bsl5cOo";

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
