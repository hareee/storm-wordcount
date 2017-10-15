package com.edi.storm.bolts;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * 
 * A bolt that add "!" to the end of the sentence.
 * @author Edison Xu
 *
 * Dec 30, 2013
 */
public class ExclaimBasicBolt extends BaseBasicBolt {

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		//String sentence = tuple.getString(0);
		String sentence = (String) tuple.getValue(0);
		String out = sentence + "!";
		collector.emit(new Values(out));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("excl_sentence"));
	}

}
