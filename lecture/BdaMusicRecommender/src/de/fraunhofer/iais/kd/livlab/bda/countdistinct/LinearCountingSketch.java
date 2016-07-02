package de.fraunhofer.iais.kd.livlab.bda.countdistinct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LinearCountingSketch implements Cloneable {

	private static final Logger LOG = LoggerFactory
			.getLogger(LinearCountingSketch.class);

	private Sketch sketch;
	private final int sketchsize;

	public LinearCountingSketch() {
		sketchsize = 200;

		sketch = new Sketch(sketchsize);

	}

	public void add(String string) {

	}

	public long estimateDistinct() {
		return 0;

	}

	@Override
	public Object clone() {
		LinearCountingSketch clone = new LinearCountingSketch();
		clone.sketch = sketch.copy();
		return clone;

	}

	public LinearCountingSketch mergeWith(LinearCountingSketch c) {

		sketch.mergeWidth(c.sketch);
		return this;

	}

	public double computeJaccardDistanceTo(LinearCountingSketch other) {
		return sketchsize;

	}

}
