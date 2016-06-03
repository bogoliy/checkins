package ua.com.myjava.checkins.domain;

public class Checkin {
	/**
	 * number of milliseconds since January 1, 1970, 00:00:00 GMT
	 */
	private long timestamp;
	/**
	 * The latitude of this location.
	 */
	public double lat;

	/**
	 * The longitude of this location.
	 */
	public double lng;

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public double getLng() {
		return lng;
	}

	public void setLng(double lng) {
		this.lng = lng;
	}

	@Override
	public String toString() {
		return "Checkin{" +
				"timestamp=" + timestamp +
				", lat=" + lat +
				", lng=" + lng +
				'}';
	}
}
