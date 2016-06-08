package ua.com.myjava.checkins.topology;

import java.util.Properties;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import backtype.storm.contrib.jms.JmsMessageProducer;
import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.contrib.jms.bolt.JmsBolt;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import ua.com.myjava.checkins.bolt.PlaceCounterBolt;
import ua.com.myjava.checkins.bolt.PlaceLookupBolt;
import ua.com.myjava.checkins.spout.CheckinsSpout;

public class CheckinsTopologyBuilder {
	public static StormTopology createTopology() {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("checkins", new CheckinsSpout(), 1);
		builder.setBolt("place-lookup", new PlaceLookupBolt(), 1).shuffleGrouping("checkins");
		builder.setBolt("place-counter", new PlaceCounterBolt(), 1).fieldsGrouping("place-lookup", new Fields("city"));
		builder.setBolt("persistor", persistor(), 1).shuffleGrouping("place-counter");
		return builder.createTopology();
	}

	private static IRichBolt persistor() {
		JmsBolt jmsBolt = new JmsBolt();
		JmsProvider provider = createJmsProvider();
		jmsBolt.setJmsProvider(provider);

		jmsBolt.setJmsMessageProducer(new JmsMessageProducer() {
			public Message toMessage(Session session, Tuple input) throws JMSException {
				String json =
						"{\"city\":\"" + input.getStringByField("city") + "\", \"count\":" + String.valueOf(input.getIntegerByField("count"))
								+ "}";
				return session.createTextMessage(json);
			}
		});

		return jmsBolt;
	}

	private static JmsProvider createJmsProvider() {
		final Context jndiContext;
		final ConnectionFactory connectionFactory;
		final Destination destination;

		 /*
		 * Create a JNDI API InitialContext object
         */
		try {
			jndiContext = getContext();
		}
		catch (NamingException e) {
			throw new RuntimeException("Could not create JNDI API context ", e);
		}
		/*
         * Look up connection factory and destination.
         */
		try {
			connectionFactory = (ConnectionFactory) jndiContext.lookup("ConnectionFactory");
			destination = (Destination) jndiContext.lookup("dynamicTopics/storm-topic");
		}
		catch (NamingException e) {
			throw new RuntimeException("JNDI API lookup failed ", e);
		}

		return new JmsProvider() {
			public ConnectionFactory connectionFactory() throws Exception {
				return connectionFactory;
			}

			public Destination destination() throws Exception {
				return destination;
			}
		};
	}

	private static Context getContext() throws NamingException {
		Properties props = new Properties();
		props.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
		props.setProperty(Context.PROVIDER_URL, "tcp://localhost:61616");
		return new InitialContext(props);
	}
}
