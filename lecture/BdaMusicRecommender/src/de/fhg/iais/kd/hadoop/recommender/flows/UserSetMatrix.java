package de.fhg.iais.kd.hadoop.recommender.flows;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Unique;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import de.fhg.iais.kd.hadoop.recommender.functions.ProjectToFields;
import de.fraunhofer.iais.kd.livlab.bda.config.BdaConstants;

public class UserSetMatrix {

	private static final String DELIMITER = ",";

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Flow getUserSetFlow(String inFile, String outFile) {

		// define source and sink Taps.
		Fields sourceFields = new Fields("uid", "datetime", "artist_mbid",
				"artist_name", "track_mbid", "track_name");
		Scheme sourceScheme = new TextDelimited(sourceFields);
		Tap source = new Hfs(sourceScheme, inFile);

		Scheme outputSehema = new TextDelimited(false, DELIMITER);

		Tap matrix = new Hfs(outputSehema, outFile + "/matrix",
				SinkMode.REPLACE);

		Pipe pipe = new Pipe("listenEvts");

		Fields targetFields = new Fields("artist_name", "uid");
		ProjectToFields projector = new ProjectToFields(targetFields);
		pipe = new Each(pipe, targetFields, projector);
		pipe = new GroupBy(pipe, new Fields("artist_name"));
		pipe = new Unique(pipe, new Fields("uid", "artist_name"));
		pipe = new GroupBy(pipe, new Fields("artist_name"));

		// Task 2
		// pipe = new Every(pipe, new ListAggregator());

		// Task 3
		pipe = new Every(pipe, new UserIdMatrixAggregator());

		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, UserSetFlow.class);

		FlowConnector flowConnector = new HadoopFlowConnector(properties);
		// FlowConnector flowConnector = new HadoopFlowConnector();

		Map<String, Tap> endPoints = new HashMap<>();
		endPoints.put("listenEvts", matrix);

		Flow flow = flowConnector.connect("uitlityMatrix", source, endPoints,
				pipe);

		// execute the flow, block until complete
		// flow.writeDOT("/tmp/debugging/plan.dot");
		return flow;
	}

	public static void main(String[] args) {
		String inFile = BdaConstants.SAMPLE_FILE;

		Flow userId = UserSetMatrix.getUserSetFlow(inFile,
				"recommender_usermatrixflowtest");
		userId.complete();
	}
}
