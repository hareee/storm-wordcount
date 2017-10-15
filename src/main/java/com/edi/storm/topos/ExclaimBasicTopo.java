package com.edi.storm.topos;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import com.edi.storm.bolts.ExclaimBasicBolt;
import com.edi.storm.bolts.PrintBolt;
import com.edi.storm.spouts.RandomSpout;

/**
 * @author Edison Xu
 * 
 *         Dec 30, 2013
 */
public class ExclaimBasicTopo {

	protected StormTopology buildTopology()
	{
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("spout", new RandomSpout());
		builder.setBolt("exclaim", new ExclaimBasicBolt(), 2).shuffleGrouping("spout");
		//builder.setBolt("exclaim", new ExclaimRichBolt(), 2).shuffleGrouping("spout");
		builder.setBolt("print", new PrintBolt(),3).shuffleGrouping("exclaim");
		return builder.createTopology();
	}
	
	public static void main(String[] args) throws Exception {
		
		ExclaimBasicTopo topo = new ExclaimBasicTopo();
		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf, topo.buildTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, topo.buildTopology());
			Utils.sleep(100000);
			//cluster.killTopology("test");
			//cluster.shutdown();
		}
	}
}
