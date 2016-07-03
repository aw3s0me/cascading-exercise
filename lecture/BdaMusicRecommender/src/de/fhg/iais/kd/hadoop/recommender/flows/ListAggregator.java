package de.fhg.iais.kd.hadoop.recommender.flows;

import java.util.LinkedList;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("serial")
public class ListAggregator extends BaseOperation<ListAggregator.Context>
		implements Aggregator<ListAggregator.Context> {

	public static class Context {
		LinkedList<String> list = new LinkedList<String>();
	}

	public ListAggregator() {
		super(1, new Fields("list"));
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
		aggregatorCall.getContext().list.add(aggregatorCall.getArguments()
				.getString(1));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void complete(FlowProcess flowProcess,
			AggregatorCall<Context> aggregatorCall) {
		aggregatorCall.getOutputCollector().add(
				new Tuple(aggregatorCall.getContext().list));
	}
}