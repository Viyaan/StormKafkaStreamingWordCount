package com.kafka.streaming.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Viyaan
 *
 */
public class WordCountBolt implements IRichBolt {
    Map<String, Integer> counters;
    private OutputCollector collector;

    
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.counters = new HashMap<String, Integer>();
        this.collector = collector;
    }

    
    public void execute(Tuple input) {
        String str = input.getString(0);
        if (!counters.containsKey(str)) {
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }
        System.out.println(" str "  + str + "count " + counters.get(str));
    }

    
    public void cleanup() {

    }

    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}