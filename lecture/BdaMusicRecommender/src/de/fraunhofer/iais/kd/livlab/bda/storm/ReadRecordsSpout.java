package de.fraunhofer.iais.kd.livlab.bda.storm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ReadRecordsSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	SpoutOutputCollector _collector;
	private BufferedReader in;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;

		File inFile = new File((String) conf.get("inputfile"));
		try {
			in = (new BufferedReader(new FileReader(inFile)));
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void nextTuple() {
		try {

			if (in != null) {
				try {
					String line = in.readLine();
					if (line != null) {
						_collector.emit(new Values(line));
					}
				} catch (IOException e) {
					try {
						in.close();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					in = null;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}