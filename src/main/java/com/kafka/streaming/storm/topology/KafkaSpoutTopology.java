package com.kafka.streaming.storm.topology;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.streaming.storm.bolt.WordCountBolt;
import com.kafka.streaming.storm.bolt.WordSpitBolt;
import com.kafka.streaming.storm.utils.ConsumerEnum;
import com.kafka.streaming.storm.utils.PropertiesLoader;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;


/**
 * @author Viyaan
 */
public class KafkaSpoutTopology {


    private final BrokerHosts brokerHosts;
    
    private static final int PARALLELISM = 1;

    public static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutTopology.class);


    public KafkaSpoutTopology(String kafkaZookeeper) {
        brokerHosts = new ZkHosts(kafkaZookeeper);
    }

    public StormTopology buildTopology(String zooKeeper,String topic,String groupId,String zkRoot) {
        ZkHosts zkHosts=new ZkHosts(zooKeeper);
        SpoutConfig kafkaConfig=new SpoutConfig(zkHosts, topic, zkRoot, groupId);
        kafkaConfig.scheme=new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.forceFromStart=true;
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout(KafkaSpout.class.getName(), new KafkaSpout(kafkaConfig), PARALLELISM);
        
        builder.setBolt(WordSpitBolt.class.getName(), new WordSpitBolt()).globalGrouping(KafkaSpout.class.getName());
        builder.setBolt(WordCountBolt.class.getName(),new WordCountBolt()).shuffleGrouping(WordSpitBolt.class.getName());
        
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {

    	PropertiesLoader loader = new PropertiesLoader();
        KafkaSpoutTopology kafkaSpoutTestTopology = new KafkaSpoutTopology(loader.getString(ConsumerEnum.ZOOKEEPER.getValue()));
        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);
        StormTopology stormTopology = kafkaSpoutTestTopology.buildTopology(loader.getString(ConsumerEnum.ZOOKEEPER.getValue()),loader.getString(ConsumerEnum.KAFKA_TOPIC.getValue()),loader.getString(ConsumerEnum.CONSUMER_GROUP.getValue()),loader.getString(ConsumerEnum.KAFKA_TOPIC.getValue()));
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("KafkaWordCountStorm", config, stormTopology);
        Thread.sleep(10000);
    }
}
