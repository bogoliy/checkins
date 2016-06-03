package ua.com.myjava.checkins;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import ua.com.myjava.checkins.topology.CheckinsTopologyBuilder;

public class LocalTopologyRunner {
	public static void main(String[] args) {
		initProxy();
		Config config = new Config();
		StormTopology topology = CheckinsTopologyBuilder.createTopology();
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("local-checkins", config, topology);
	}


	private static void initProxy() {
		System.setProperty("http.proxyHost", "127.0.0.1");
		System.setProperty("http.proxyPort", "3129");
		System.setProperty("https.proxyHost", "127.0.0.1");
		System.setProperty("https.proxyPort", "3129");
	}
}
