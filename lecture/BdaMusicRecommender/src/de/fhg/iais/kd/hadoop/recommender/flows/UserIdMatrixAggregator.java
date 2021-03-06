package de.fhg.iais.kd.hadoop.recommender.flows;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("serial")
public class UserIdMatrixAggregator extends
		BaseOperation<UserIdMatrixAggregator.Context> implements
		Aggregator<UserIdMatrixAggregator.Context> {

	public static class Context {
		boolean[] row = new boolean[1000];
	}

	public UserIdMatrixAggregator() {
		super(1, new Fields("row"));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void start(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		aggregatorCall.setContext(new Context());
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void aggregate(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		aggregatorCall.getContext().row[Integer.parseInt(aggregatorCall
				.getArguments().getString(1).substring(7)) - 1] = true;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void complete(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		Context context = aggregatorCall.getContext();

		StringBuilder sb = new StringBuilder();
		for (boolean b : context.row) {
			sb.append(b ? "1," : "0,");
		}
		aggregatorCall.getOutputCollector().add(new Tuple(sb.toString()));
	}
}
