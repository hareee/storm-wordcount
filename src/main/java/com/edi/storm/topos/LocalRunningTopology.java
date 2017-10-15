package com.edi.storm.topos;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.utils.Utils;

/**
 * A sample topology running only under Local cluster model.
 * 
 * @author Edison Xu
 * 
 *         Jan 13, 2014
 */
public class LocalRunningTopology extends ExclaimBasicTopo {

	public static void main(String[] args) throws Exception {

		LocalRunningTopology topo = new LocalRunningTopology();
		Config conf = new Config();
		conf.setDebug(false);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, topo.buildTopology());
		Utils.sleep(100000);
		cluster.killTopology("test");
		cluster.shutdown();
	}
}
