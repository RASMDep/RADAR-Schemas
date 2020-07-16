package org.radarcns.schema.registration;

import static org.radarbase.util.Strings.isNullOrEmpty;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import javax.validation.constraints.NotNull;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Node;
import org.radarcns.schema.CommandLineApp;
import org.radarcns.schema.util.SubCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers Kafka topics with Zookeeper.
 */
public class KafkaTopics extends AbstractTopicRegistrar {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTopics.class);

    @NotNull
    private final AdminClient kafkaClient;
    @NotNull
    private final String bootstrapServers;
    private boolean initialized;

    /**
     * Create Kafka topics registration object with given properties.
     */
    public KafkaTopics(@NotNull Properties adminProperties) {
        kafkaClient = AdminClient.create(adminProperties);
        bootstrapServers = adminProperties.getProperty("bootstrap.servers");
        initialized = false;
    }

    /**
     * Wait for brokers to become available. This uses a polling mechanism,
     * waiting for at most 200 seconds.
     *
     * @param brokers number of brokers to wait for
     * @return whether the brokers where available
     * @throws InterruptedException     when waiting for the brokers is interrupted.
     */
    public boolean initialize(int brokers) throws InterruptedException {
        int sleep = 2;
        int numTries = 20;
        int numBrokers = 0;

        for (int tries = 0; tries < numTries; tries++) {
            numBrokers = getNumberOfBrokers();

            if (numBrokers >= brokers) {
                break;
            } else {
                if (tries < numTries - 1) {
                    logger.warn("Only {} out of {} Kafka brokers available. Waiting {} seconds.",
                            numBrokers, brokers, sleep);
                    Thread.sleep(sleep * 1000L);
                    sleep = Math.min(MAX_SLEEP, sleep * 2);
                } else {
                    logger.error("Only {} out of {} Kafka brokers available."
                            + " Failed to wait on all brokers.", numBrokers, brokers);
                }
            }
        }

        initialized = numBrokers >= brokers;
        return initialized && refreshTopics();
    }

    @Override
    public void ensureInitialized() {
        if (!initialized) {
            throw new IllegalStateException("Manager is not initialized yet");
        }
    }

    private Collection<Node> currentBrokers() throws InterruptedException {
        try {
            return kafkaClient.describeCluster().nodes().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            logger.error("Cannot describe Kafka cluster", e);
            return Collections.emptyList();
        }
    }

    public String getBootstrapServers() {
        ensureInitialized();
        return bootstrapServers;
    }

    /**
     * Get current number of Kafka brokers according to Zookeeper.
     *
     * @return number of Kafka brokers
     */
    public int getNumberOfBrokers() throws InterruptedException {
        return currentBrokers().size();
    }

    @NotNull
    @Override
    public AdminClient getKafkaClient() {
        ensureInitialized();
        return kafkaClient;
    }

    /**
     * Create a KafkaTopics command to register topics from the command line.
     */
    public static SubCommand command() {
        return new KafkaTopicsCommand();
    }

    static class KafkaTopicsCommand implements SubCommand {
        @Override
        public String getName() {
            return "create";
        }

        @Override
        public int execute(Namespace options, CommandLineApp app) {
            int brokers = options.getInt("brokers");
            short replication = options.getShort("replication");

            if (brokers < replication) {
                logger.error("Cannot assign a replication factor {}"
                        + " higher than number of brokers {}", replication, brokers);
                return 1;
            }

            int partitions = options.getInt("partitions");
            Properties properties;
            try {
                properties = adminClientProperties(
                    options.getString("bootstrap-servers"),
                    options.getString("property-file"));
            } catch (IOException ex) {
                logger.error("Failed to read properties file: {}", ex.toString());
                return 1;
            }

            try (KafkaTopics topics = new KafkaTopics(properties)) {
                if (!topics.initialize(brokers)) {
                    logger.error("Kafka brokers not yet available. Aborting.");
                    return 1;
                }
                return topics.createTopics(app.getCatalogue(), partitions, replication,
                        options.getString("topic"), options.getString("match"));

            } catch (InterruptedException e) {
                logger.error("Cannot retrieve number of addActive Kafka brokers."
                        + " Please check that Kafka is running.");
                return 1;
            }
        }

        @Override
        public void addParser(ArgumentParser parser) {
            parser.description("Create all topics that are missing on the Kafka server.");
            parser.addArgument("-p", "--partitions")
                    .help("number of partitions per topic")
                    .type(Integer.class)
                    .setDefault(3);
            parser.addArgument("-r", "--replication")
                    .help("number of replicas per data packet")
                    .type(Short.class)
                    .setDefault((short) 3);
            parser.addArgument("-b", "--brokers")
                    .help("number of brokers that are expected to be available.")
                    .type(Integer.class)
                    .setDefault(3);
            parser.addArgument("-t", "--topic")
                    .help("register the schemas of one topic")
                    .type(String.class);
            parser.addArgument("-m", "--match")
                    .help("register the schemas of all topics matching the given regex"
                            + "; does not do anything if --topic is specified")
                    .type(String.class);
            parser.addArgument("-P", "--property")
                    .help("Admin client properties")
                    .type(List.class);
            parser.addArgument("-f", "--property-file")
                    .help("Admin client property file")
                    .type(String.class);
            parser.addArgument("-s", "--bootstrap-servers")
                    .help("Kafka hosts and ports, comma-separated")
                    .type(String.class);
            SubCommand.addRootArgument(parser);
        }

        public static Properties adminClientProperties(
                String bootstrapServers,
                String filePathString) throws IOException {
            Properties properties = new Properties();

            if (!isNullOrEmpty(filePathString)) {
                Path filePath = Paths.get(filePathString);
                if (Files.exists(filePath)) {
                    try (Reader reader = Files.newBufferedReader(filePath)) {
                        properties.load(reader);
                    }
                }
            }
            if (bootstrapServers != null) {
                properties.put("bootstrap.servers", bootstrapServers);
            }

            return properties;
        }
    }
}
