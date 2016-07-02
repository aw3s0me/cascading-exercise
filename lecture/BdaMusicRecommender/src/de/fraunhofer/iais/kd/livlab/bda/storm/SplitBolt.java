package de.fraunhofer.iais.kd.livlab.bda.storm;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 *
 *
 */
public class SplitBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	OutputCollector collector;

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void execute(Tuple tuple) {

		String text = tuple.getString(0); // full line

		String[] split = text.split("\t");
		if (split.length == 6) {

			// LastFmDataRecord(String user_id, String atimestamp, String artid,
			// String artname, String traid, String traname) {
			Values values = new Values();
			values.add(split[0].replaceAll("'", "")); // userid
			values.add(split[2].replaceAll("'", "")); // artid
			values.add(split[3].replaceAll("'", "")); // artname

			collector.emit(values);

		}

		collector.ack(tuple);
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("userid", "artid", "artname"));
	}

	@Override
	public Map getComponentConfiguration() {
		return null;
	}

}
