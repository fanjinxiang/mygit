package com.odianyun.storm;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by user on 2017/8/29.
 */
public class WordNormalizer extends BaseRichBolt {

    private OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String sentence = input.getString(0);
        String[]words = sentence.split(" ");
        for(String word:words){
            word = word.trim();
            if(word!=null && !word.equals("")){
                collector.emit(input,new Values(word));
            }
        }
        // 对元组做出应答
        collector.ack(input);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
