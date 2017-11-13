package com.sponge.storm.kafka;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.util.Properties;

public class TopicMsgTopology {
    public static void main(String[] args) throws Exception {
        // 配置Zookeeper地址
        BrokerHosts brokerHosts = new ZkHosts("sdc2.sefon.com:2181");
        // 配置Kafka订阅的Topic，以及zookeeper中数据节点目录和名字
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "msgTopic1", "/topology/root", "topicMsgTopology");
        spoutConfig.securityProtocol = "PLAINTEXTSASL";

        // 配置KafkaBolt中的kafka.broker.properties
        Config conf = new Config();
        Properties props = new Properties();
        // 配置Kafka broker地址
        props.put("bootstrap.servers", "sdc1.sefon.com:6667,sdc2.sefon.com:6667");
        props.put("security.protocol", "PLAINTEXTSASL");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaBolt boltKafka = new KafkaBolt()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector("msgTopic2"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());


        // 配置KafkaBolt生成的topic

        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("msgKafkaSpout", new KafkaSpout(spoutConfig));
        builder.setBolt("msgSentenceBolt", new TopicMsgBolt()).shuffleGrouping("msgKafkaSpout");
        builder.setBolt("msgKafkaBolt", boltKafka).shuffleGrouping("msgSentenceBolt");
        if (args.length == 0) {
            String topologyName = "kafkaTopicTopology";
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, builder.createTopology());
            Utils.sleep(100000);
            cluster.killTopology(topologyName);
            cluster.shutdown();
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        }
    }
}
