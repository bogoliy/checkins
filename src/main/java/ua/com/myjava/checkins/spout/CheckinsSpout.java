package ua.com.myjava.checkins.spout;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.google.gson.Gson;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import ua.com.myjava.checkins.domain.Checkin;

public class CheckinsSpout extends BaseRichSpout {
	private SpoutOutputCollector outputCollector;
	private Server server;
	private BlockingQueue<Checkin> checkins = new LinkedBlockingQueue<Checkin>();

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("checkin"));
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector outputCollector) {
		this.outputCollector = outputCollector;
		server = new Server(18080);
		try {
			server.setHandler(new ClientsHandler());

			server.start();
			server.dumpStdErr();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void nextTuple() {
		if (checkins.size() > 0) {
			Checkin checkin = checkins.remove();
			outputCollector.emit(new Values(checkin));
			System.out.print(checkin);
		} else {
			try {
				Thread.sleep(1);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void close() {
		super.close();
		try {
			server.stop();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	private class ClientsHandler extends AbstractHandler {
		private final Gson gson = new Gson();

		public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse)
				throws IOException, ServletException {
			BufferedReader reader = null;
			try {
				reader = httpServletRequest.getReader();
				StringBuilder stringBuilder = new StringBuilder();
				char[] charsBuffer = new char[128];
				int bytesRead;
				while ((bytesRead = reader.read(charsBuffer)) > 0) {
					stringBuilder.append(charsBuffer, 0, bytesRead);
				}
				Checkin checkin = gson.fromJson(stringBuilder.toString(), Checkin.class);
				checkins.add(checkin);
				request.setHandled(true);
			}
			finally {
				if (reader != null) {
					reader.close();
				}
			}
		}
	}
}
