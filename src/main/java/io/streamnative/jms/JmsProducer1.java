package io.streamnative.jms;


import io.streamnative.pulsar.jms.PulsarConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Topic;
import java.util.HashMap;
import java.util.Map;

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
public class JmsProducer1 {

    private static final String BROKER_SERVICE_URL =
            "pulsar+ssl://pc-c3720b92.aws-use2-production-snci-pool-kid.streamnative.aws.snio.cloud:6651";
    private static final String WEB_SERVICE_URL =
            "https://pc-c3720b92.aws-use2-production-snci-pool-kid.streamnative.aws.snio.cloud";
    private static final String TOKEN =
            "eyJhbGciOiJSUzI1NiIsImtpZCI6InByb2R1Y3Rpb24ta2V5LTIwMjUtMTItdjEiLCJ0eXAiOiJKV1QifQ.eyJhdWQiOlsidXJuOnNuOmNsb3VkOnNuY2xvdWQiXSwiZXhwIjoxNzc4MjA4OTM5LCJpYXQiOjE3NzU2MTY5NDAsImlzcyI6Imh0dHBzOi8vYXBpa2V5cy5zdHJlYW1uYXRpdmUuY2xvdWQvIiwianRpIjoiMGYxMzQ4YzU5ZGE0NDRkZWExOWM2ODM2OTk4MDM5ZTIiLCJzY29wZSI6W10sInN1YiI6ImFkbWluQHNuY2xvdWQuYXV0aC5zdHJlYW1uYXRpdmUuY2xvdWQifQ.HdnAGo13NF3458uYTMS7Mur807ZYdDoGRCZx6q_gtCnp64ybXGoE12qrsRG3t4l3lQDLoPfrJMD9oGVahs_XpOxRG_wKv2kgQxTkyJuYf3SMk1bpMoP-UJNWIk-FBvPAQT_rc59-uRxOXO9JWM2QyaqR1ze0zUtwOqppvk979rjdcDrchbOaRxh_4F6-9Iy9j4LIwIzsPaJnoWnPNamI3UblfjQpVy-nQYh3PEILicLLoKeegOgSbVpFh6jOUBs2CgDiW0U6wQBgPuW7HlVHtoOZyYoL1XiSV4WtKQ6L6oyLcUtnlIG0pLzAL9u6NO6AaekdiG5Uo5w06QPi-wT7zcbt77vls4nVI1ZGFpIv-zyAUC9XCbPIRKn3TD8o7N_VpbpNG7DVwHKgnT0s1iPEf4zlaDXGF8d9Xf6_K4xFIAk4vKYHqXzhcsc6EyzDLPtUIUQArelqEAO0MIKBSmlJEKus8KEr4umFX5JU7YGSxS0rWCubDO4YdIirZDzuPkuULUbVFa7avFcfHqOmQ3BIYnETeTyUz6WXKDJn88PtXRt4NWCCiIdt82qSbcs8-SlmMVaZYsJ7VtU8soNpCIetGT5FKxDGxNua1LAholmUafTIJjp0XneYiUhGEfc_zA9WPucGMkclg9WtUnPKNW1rKf7XrnAF-iSQTXO3Bsl5cOo";

    public static void main(String[] args) throws Exception {
        String topicName   = args.length > 0 ? args[0] : "persistent://public/default/input-1";
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
                String body = "hello-jms-test1-" + i;
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
