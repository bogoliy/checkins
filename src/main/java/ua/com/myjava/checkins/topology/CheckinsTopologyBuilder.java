package ua.com.myjava.checkins.topology;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import ua.com.myjava.checkins.bolt.Persistor;
import ua.com.myjava.checkins.bolt.PlaceCounterBolt;
import ua.com.myjava.checkins.bolt.PlaceLookupBolt;
import ua.com.myjava.checkins.spout.CheckinsSpout;

public class CheckinsTopologyBuilder {
	public static StormTopology createTopology() {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("checkins", new CheckinsSpout(), 1);
		builder.setBolt("place-lookup", new PlaceLookupBolt(), 1).shuffleGrouping("checkins");
		builder.setBolt("place-counter", new PlaceCounterBolt(), 1).fieldsGrouping("place-lookup", new Fields("city"));
		builder.setBolt("persistor", new Persistor(), 1).shuffleGrouping("place-counter");
		return builder.createTopology();
	}
}
