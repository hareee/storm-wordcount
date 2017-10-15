package com.edi.storm.topos;

import java.io.File;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;

import com.edi.storm.util.EJob;

/**
 * @author Edison Xu
 * 
 *         Jan 13, 2014
 */
public class RemoteRunningTopology extends ExclaimBasicTopo {

	public static void main(String[] args) throws Exception {

		String topoName = "test";
		RemoteRunningTopology topo = new RemoteRunningTopology();
		Config conf = new Config();
		conf.setDebug(false);

		File jarFile = EJob.createTempJar(RemoteRunningTopology.class.getClassLoader().getResource("").getPath());
        ClassLoader classLoader = EJob.getClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
        
		//System.setProperty("storm.jar", Class.forName("com.edi.storm.topos.RemoteRunningTopology").getProtectionDomain().getCodeSource().getLocation().getPath());
        System.setProperty("storm.jar", jarFile.toString());
		conf.setNumWorkers(5);
		conf.setDebug(false);
		//conf.put(Config.NIMBUS_HOST, "dsjpt-6new");
		//conf.put(Config.NIMBUS_THRIFT_PORT, 8889);
		conf.put(Config.NIMBUS_SEEDS, "dsjpt-6new");
		StormSubmitter.submitTopology(topoName, conf, topo.buildTopology());
	}
}
