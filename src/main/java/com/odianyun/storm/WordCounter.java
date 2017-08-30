package com.odianyun.storm;



import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by user on 2017/8/29.
 */
public class WordCounter extends BaseRichBolt {
    private Map<String,Integer> counts = new HashMap<String, Integer>();
    private OutputCollector collector;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String word = input.getString(0);
        if(!counts.containsKey(word)){
            counts.put(word,1);
        }else{
            Integer c = counts.get(word) + 1;
            counts.put(word,c);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void cleanup(){
        for(Map.Entry<String,Integer> entry : counts.entrySet()){
            System.out.println(entry.getKey()+":"+entry.getValue());
        }
    }
}
