package ua.com.myjava.checkins.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PlaceCounterBolt extends BaseBasicBolt {

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, TimeUnit.MINUTES.toSeconds(15));
		return conf;
	}

	public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
		if (isTickTuple(tuple)) {
			for (Map.Entry<String, Long> entry : cityToCheckinsCountMap.entrySet()) {
				outputCollector.emit(new Values(entry.getKey(), entry.getValue()));
			}
		} else {
			Long timestamp = tuple.getLongByField("timestamp");
			String city = tuple.getStringByField("city");
			long checkInterval = timestamp / TimeUnit.MINUTES.toMillis(15);
			long currentInterval = System.currentTimeMillis() / TimeUnit.MINUTES.toMillis(15);
			if (checkInterval == currentInterval) {
				Long count = cityToCheckinsCountMap.get(city);
				if (count == null) {
					count = 0L;
				}
				cityToCheckinsCountMap.put(city, ++count);
				System.out.print(city + " - " + count);
			}
		}
	}

	private Map<String, Long> cityToCheckinsCountMap;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		cityToCheckinsCountMap = new HashMap<String, Long>();
	}

	private boolean isTickTuple(Tuple tuple) {
		String sourceComponent = tuple.getSourceComponent();
		String sourceStreamId = tuple.getSourceStreamId();
		return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID) && sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("city", "count"));
	}

}
