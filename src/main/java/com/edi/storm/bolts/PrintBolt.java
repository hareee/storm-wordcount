package com.edi.storm.bolts;

import java.util.Map;

import com.edi.storm.util.PrintHelper;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * Print the String received
 * 
 * @author Edison Xu
 *
 * Dec 30, 2013
 */
public class PrintBolt extends BaseBasicBolt {

    private int indexId;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.indexId = context.getThisTaskIndex();
    }

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String rec = tuple.getString(0);
		PrintHelper.print(String.format("Bolt[%d] String recieved: %s",this.indexId, rec));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// do nothing
	}

}
