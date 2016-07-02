package de.fhg.iais.kd.hadoop.recommender.flows;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import de.fhg.iais.kd.hadoop.recommender.userset.UserSet;

/**
 * Custom aggregator for collecting user ids
 * 
 * @author akorovin
 * 
 */
public class UserIdAggregator extends BaseOperation<UserIdAggregator.Context>
		implements Aggregator<UserIdAggregator.Context> {

	/**
	 * Contains value of aggregator
	 * 
	 * @author akorovin
	 * 
	 */
	public class Context {
		UserSet user = new UserSet();
	}

	public UserIdAggregator(Fields fields) {
		super(1, fields);
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
	@Override
	public void aggregate(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		// TODO Auto-generated method stub
		// get the current argument values
		TupleEntry arguments = aggregatorCall.getArguments();

		// get the context for this grouping
		Context context = aggregatorCall.getContext();

		// update the context object
		String userName = arguments.getObject(0).toString().trim();
		context.user.add(userName);
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
		String transformedUserSet = context.user.toString();

		result.add(transformedUserSet);
		// return the result Tuple
		aggregatorCall.getOutputCollector().add(result);
	}

}
