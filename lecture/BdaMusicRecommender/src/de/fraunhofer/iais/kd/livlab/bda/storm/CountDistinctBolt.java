package de.fraunhofer.iais.kd.livlab.bda.storm;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import de.fraunhofer.iais.kd.livlab.bda.countdistinct.detector.CountDistinctDetecor;

/**
 *
 *
 */
public class CountDistinctBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	OutputCollector collector;
	private CountDistinctDetecor detector;

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		detector = new CountDistinctDetecor();

	}

	@Override
	public void execute(Tuple tuple) {

		String userid = tuple.getStringByField("userid");
		String artid = tuple.getStringByField("artid");
		String artname = tuple.getStringByField("artname");

		String[] result = detector.process(userid, artid, artname);

		if (result != null) {
			System.out.println("Userid:" + result[0] + " artname:" + result[1]);
		}

		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("artname", "clusterid"));

	}

}
