package org.pathirage.fdbench.datagen.simple;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.util.Properties;

public class SimpleKafkaMessageGenerator {
  public static void main(String[] args) {
    Options options = new Options();
    JCommander jc = new JCommander(options, args);

    Properties props = new Properties();
    props.put("bootstrap.servers", options.kafkaBrokers);
    props.put("key.serializer", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", "test.kafka.SimplePartitioner");
    props.put("request.required.acks", "1");

  }


  public static class Options {

    @Parameter(names = {"--brokers", "-b"}, description = "Kafka brokers")
    public String kafkaBrokers;

    @Parameter(names =  {"--zk", "-z"}, description = "Zookeeper Connection String")
    public String zkConnecitonStr;

    @Parameter(names = {"--message-size", "-s"}, description = "Individual message size")
    public int messageSize;

    @Parameter(names =  {"--message-count", "-m"}, description = "Number of messahes to send")
    public int messages;
  }
}
