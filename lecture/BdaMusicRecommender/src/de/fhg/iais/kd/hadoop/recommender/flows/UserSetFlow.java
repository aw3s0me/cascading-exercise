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
import de.fraunhofer.iais.kd.livlab.bda.config.BdaConstants;

public class UserSetFlow {

	private static final String DELIMITER = "|";

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Flow getUserSetFlow(String inFile, String outFile) {
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

		// using aggregator. to get format artist: [users]
		Aggregator userIdAggregator = new UserIdAggregator(new Fields("users"));
		pipe = new Every(pipe, userIdAggregator);

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

		Flow userId = UserSetFlow.getUserSetFlow(inFile,
				"recommender_usersetflowtest");
		userId.complete();
	}
}
