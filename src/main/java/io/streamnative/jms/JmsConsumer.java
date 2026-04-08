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
            "pulsar+ssl://pc-c3720b92.aws-use2-production-snci-pool-kid.streamnative.aws.snio.cloud:6651";
    private static final String WEB_SERVICE_URL =
            "https://pc-c3720b92.aws-use2-production-snci-pool-kid.streamnative.aws.snio.cloud";
    private static final String TOKEN =
            "eyJhbGciOiJSUzI1NiIsImtpZCI6InByb2R1Y3Rpb24ta2V5LTIwMjUtMTItdjEiLCJ0eXAiOiJKV1QifQ.eyJhdWQiOlsidXJuOnNuOmNsb3VkOnNuZGV2Il0sImV4cCI6MTc3ODE1NjUyMiwiaWF0IjoxNzc1NTY0NTIzLCJpc3MiOiJodHRwczovL2FwaWtleXMuc3RyZWFtbmF0aXZlLmNsb3VkLyIsImp0aSI6ImZmOTc1ZWY0M2EzZDQyNTViYTk0OTE4ZWUwNDY3YWYwIiwic2NvcGUiOltdLCJzdWIiOiJhZG1pbkBzbmRldi5hdXRoLnN0cmVhbW5hdGl2ZS5jbG91ZCJ9.e2WSFuMpIBfrnbnjrv4M-iJ-b0aKYNY5Zi3S20IqJUlfaZvCHPqHO39_Uv5TalAuJTU0b3z41bZokJ2lIvwdLos0ZvNmzJ3gk577SDNPDHuvRx49kn-qVAZ8Js5P0jdYaQ3nogKUM4DrEf63qLZmh13cxN6PrOkjpX3kLvEjlocyaOJVZmdgYzKfv6nIbQDolnx242Njoz5hUfe0KeqJmTMwbNSWrEeJknMZs5VFtoxSkuM8CCtw1FysWF_szHsP7p6B81pF9YtH4xR8mjYmLLvXqLwLgDlADt4yAwq9Zr0wuzKcG9Ned37OCub_mMADD9uZpxJ_SaSmdYb_s3wTDHmawp-j12gea5ZyakvWAXm38ZWfwC_81OyB0UrmZEpp7MThgAFBejWrbDyz15xpIVgVm3tC5_wbzRqJprfFYX8hWNvt3TnHDYHBIul4zfXedLmvgvBltJ1M4JFtGZp8BJ_I0u3WMfCQgeXtuRTZbcVsoAqlk3Tx4FPKLcqo1oWRnrMOefUNNJ_MR2DU2kKRJ6jX5M--uq5OjvMOQiLfem77J-KL5E0R02v9-O6vBoqfDItn8bk-anYdqRYt-X52YjIn5CrQeyKYCno7SZgKFChrUU74MW35DUyLyLrL2_TgGXPSKBXr3E19YwumzPD_geUWUtDYjSfVxSn45USi3Dk";

    public static void main(String[] args) throws Exception {
        String topicName   = args.length > 0 ? args[0] : "persistent://public/default/jms-test";
        int    numMessages = args.length > 1 ? Integer.parseInt(args[1]) : 5;
        long   timeoutMs   = args.length > 2 ? Long.parseLong(args[2]) : 5000L;

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
