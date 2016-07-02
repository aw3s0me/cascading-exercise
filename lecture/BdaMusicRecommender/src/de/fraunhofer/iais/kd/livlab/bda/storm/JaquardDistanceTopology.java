package de.fraunhofer.iais.kd.livlab.bda.storm;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import de.fraunhofer.iais.kd.livlab.bda.config.BdaConstants;

/**
 * @author mmock
 */
public class JaquardDistanceTopology {

	private static final Logger LOG = LoggerFactory
			.getLogger(JaquardDistanceTopology.class);

	public static void main(String[] args) throws InterruptedException,
			AlreadyAliveException, InvalidTopologyException, IOException {

		Config conf = new Config();
		conf.setDebug(false);
		// conf.put("inputfile",
		// "/home/livlab/data/lastfm-dataset-1K/cleaned1.tsv");
		conf.put("inputfile", BdaConstants.SAMPLE_FILE);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new ReadRecordsSpout(), 1);
		builder.setBolt("splitBolt", new SplitBolt(), 1).shuffleGrouping(
				"spout");
		builder.setBolt("countDistinctBolt", new CountDistinctBolt(), 3)
				.shuffleGrouping("splitBolt"); //

		if (args != null && args.length == 3) {

			conf.setNumWorkers(4);

			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
			LOG.info("Topology submitted");
		} else {
			conf.setMaxTaskParallelism(4);

			LocalCluster cluster = new LocalCluster();

			cluster.submitTopology("test", conf, builder.createTopology());
			LOG.info("Topology submitted locallly");

			while (true) {
				Thread.sleep(600000);
			}

			// cluster.shutdown();
		}
	}
}
