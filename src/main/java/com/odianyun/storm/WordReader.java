package com.odianyun.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * Created by user on 2017/8/29.
 */
public class WordReader implements IRichSpout {

    private BufferedReader reader;
    private SpoutOutputCollector collector;
    private boolean completed = false;

    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector collector) {
        String fileName = conf.get("fileName").toString();
        InputStream stream = WordReader.class.getClassLoader().getResourceAsStream(fileName);
        reader = new BufferedReader(new InputStreamReader(stream));
        this.collector = collector;
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
        if(completed){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("Spout run over！");
                //e.printStackTrace();
            }
            return;
        }
        String str;
        int i = 0;
        try {
            while((str = reader.readLine())!=null){
                System.out.println("read the lines,line number is :"+ i++);
                /**
                 * 按行发布一个新值
                 */
                collector.emit(new Values(str),str);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            completed = true;
        }
    }

    public void ack(Object o) {

    }

    public void fail(Object o) {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        System. out.println("WordReader.declareOutputFields(OutputFieldsDeclarer declarer)");
        outputFieldsDeclarer.declare(new Fields("line"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
