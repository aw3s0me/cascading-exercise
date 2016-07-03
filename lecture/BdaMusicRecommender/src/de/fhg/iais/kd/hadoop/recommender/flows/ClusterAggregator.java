package de.fhg.iais.kd.hadoop.recommender.flows;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import de.fhg.iais.kd.hadoop.recommender.userset.UserSet;
import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModel;
import de.fraunhofer.iais.kd.livlab.bda.clustermodel.UserSetClusterModel;

/**
 * Custom aggregator for calculating clusters
 * 
 * @author akorovin
 * 
 */
public class ClusterAggregator extends BaseOperation<ClusterAggregator.Context>
		implements Aggregator<ClusterAggregator.Context> {

	private final ClusterModel model;

	/**
	 * Contains value of aggregator
	 * 
	 * @author akorovin
	 * 
	 */
	public class Context {
		UserSet user = new UserSet();
	}

	public ClusterAggregator(Fields fields, ClusterModel model) {
		super(1, fields);
		this.model = model;
	}

	/**
	 * Called before group starts Taken from example (cascading documentation)
	 */
	@Override
	public void start(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		// TODO Auto-generated method stub
		// get the group values
		TupleEntry group = aggregatorCall.getGroup();
		// initialize context for a new user set
		Context context = new Context();
		// set the context
		aggregatorCall.setContext(context);
	}

	/**
	 * Called for each group member
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void aggregate(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		// TODO Auto-generated method stub
		// get the current argument values
		TupleEntry arguments = aggregatorCall.getArguments();

		// get the context for this grouping
		Context context = aggregatorCall.getContext();

		// update the context object
		int index = Integer.parseInt(aggregatorCall.getArguments().getString(0)
				.substring(7));
		// String userName = arguments.getObject(0).toString().trim();
		// context.user.add(userName);
		context.user.add(Integer.toString(index));
	}

	/**
	 * Run after system is finished
	 */
	@Override
	public void complete(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		// TODO Auto-generated method stub
		Context context = aggregatorCall.getContext();

		// create a Tuple to hold our result values
		Tuple result = new Tuple();

		// insert some values into the result Tuple based on the context
		UserSetClusterModel userSetModel = new UserSetClusterModel(model);
		String cluster = userSetModel.findClosestCluster(context.user);

		result.add(cluster + " : ");
		// return the result Tuple
		aggregatorCall.getOutputCollector().add(result);
	}

}
