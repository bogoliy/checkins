package ua.com.myjava.checkins.bolt;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class Persistor extends BaseBasicBolt {

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {

	}


	@Override
	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
