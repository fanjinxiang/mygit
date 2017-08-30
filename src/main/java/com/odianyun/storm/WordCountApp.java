package com.odianyun.storm;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Created by user on 2017/8/29.
 */
public class WordCountApp {
    public static void main(String[]args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wordreader",new WordReader());
        builder.setBolt("wordnormalizer",new WordNormalizer()).shuffleGrouping("wordreader");
        builder.setBolt("wordcounter",new WordCounter()).fieldsGrouping("wordnormalizer",new Fields("word"));
        StormTopology topology = builder .createTopology();
        //配置

        Config config = new Config();
        String fileName ="words.txt" ;
        config.put("fileName" , fileName );
        config.setDebug(false);

        //运行拓扑
        if(args !=null&&args.length>0){ //有参数时，表示向集群提交作业，并把第一个参数当做topology名称
            StormSubmitter.submitTopology(args[0], config, topology);
        } else{//没有参数时，本地提交
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("wordcountapp" , config , topology);
            Thread. sleep(10000);
            localCluster.shutdown();
        }
    }
}
