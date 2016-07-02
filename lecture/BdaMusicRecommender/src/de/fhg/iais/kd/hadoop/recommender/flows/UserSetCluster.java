package de.fhg.iais.kd.hadoop.recommender.flows;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Aggregator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import de.fhg.iais.kd.hadoop.recommender.functions.ProjectToFields;
import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModel;
import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModelFactory;
import de.fraunhofer.iais.kd.livlab.bda.config.BdaConstants;

public class UserSetCluster {

	private static final String DELIMITER = "|";

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Flow getUserSetFlow(String inFile, String outFile,
			ClusterModel model) {
		Fields sourceFields = new Fields("uid", "datetime", "artist_mbid",
				"artist_name", "track_mbid", "track_name");

		// define source (input) tap
		Scheme sourceScheme = new TextDelimited(sourceFields);
		Tap source = new Hfs(sourceScheme, inFile);

		// define output tap
		Scheme outputScheme = new TextDelimited(false, DELIMITER);
		Tap output = new Hfs(outputScheme, outFile + "/userset",
				SinkMode.REPLACE);

		Pipe pipe = new Pipe("listenEvts");

		// Filter out empty mbids
		Fields targetFields = new Fields("uid", "artist_name");
		ProjectToFields projector = new ProjectToFields(targetFields);
		pipe = new Each(pipe, targetFields, projector);

		// Group by the same artist name
		pipe = new GroupBy(pipe, new Fields("artist_name"));

		Aggregator aggregator = new ClusterAggregator(new Fields("cluster_id"),
				model);
		pipe = new Every(pipe, aggregator);

		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties,
				BuildInteractionMatrix.class);

		FlowConnector flowConnector = new HadoopFlowConnector(properties);
		// FlowConnector flowConnector = new HadoopFlowConnector();

		Map<String, Tap> endPoints = new HashMap<>();
		endPoints.put("listenEvts", output);

		Flow flow = flowConnector.connect("uitlityMatrix", source, endPoints,
				pipe);
		return flow;
	}

	public static void main(String[] args) {
		String inFile = BdaConstants.SAMPLE_FILE;
		String clusterFile = BdaConstants.CLUSTER_MODEL;
		Flow userId = UserSetCluster.getUserSetFlow(inFile,
				"recommender_usersetclusterflowtest",
				ClusterModelFactory.readFromCsvResource(clusterFile));
		userId.complete();
	}
}
