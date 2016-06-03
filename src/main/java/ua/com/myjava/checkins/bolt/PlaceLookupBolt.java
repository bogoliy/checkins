package ua.com.myjava.checkins.bolt;

import java.util.Map;

import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;
import com.google.maps.model.AddressComponent;
import com.google.maps.model.AddressComponentType;
import com.google.maps.model.GeocodingResult;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import ua.com.myjava.checkins.domain.Checkin;

public class PlaceLookupBolt extends BaseBasicBolt {

	public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
		Checkin checkin = (Checkin) tuple.getValueByField("checkin");
		String city;
		if ((city = resolveCity(checkin.getLat(), checkin.getLng())) != null) {
			System.out.print(city);
			outputCollector.emit(new Values(checkin.getTimestamp(), city));
		}

	}

	private String resolveCity(double lat, double lng) {
		String address = lat + "," + lng;
		GeoApiContext context = new GeoApiContext().setApiKey("AIzaSyDXW0-JPp8ClhDe283rdZbvDEwyDeFhRxI");
		GeocodingResult[] results = null;
		try {
			results = GeocodingApi.geocode(context, address).await();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		if (results != null && results.length > 0) {
			GeocodingResult firstResult = results[0];
			for (AddressComponent addressComponent : firstResult.addressComponents) {
				for (AddressComponentType type : addressComponent.types) {
					if (type == AddressComponentType.LOCALITY) {
						return addressComponent.longName;
					}
				}
			}
		}
		return null;
	}

	public void declareOutputFields(OutputFieldsDeclarer fieldsDeclarer) {
		fieldsDeclarer.declare(new Fields("timestamp", "city"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

	}
}

