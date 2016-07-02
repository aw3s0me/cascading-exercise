package de.fraunhofer.iais.kd.livlab.bda.tests;

import junit.framework.Assert;

import org.junit.Test;

import de.fraunhofer.iais.kd.livlab.bda.countdistinct.LinearCountingSketch;

public class LinearCountingSketchTest {
	@Test
	public void testCountDistinctSketchBased() {
		LinearCountingSketch countDistinct = new LinearCountingSketch();
		countDistinct.add("d1");
		countDistinct.add("d1");
		countDistinct.add("d2");

		Assert.assertEquals(2, countDistinct.estimateDistinct());

	}

	@Test
	public void testCountMergeSketchBased() {
		LinearCountingSketch s1 = new LinearCountingSketch();
		LinearCountingSketch s2 = new LinearCountingSketch();

		s1.add("d1");
		s1.add("d1");
		s1.add("d2");

		s2.add("d2");
		s2.add("d2");
		s2.add("d3");

		s1.mergeWith(s2);

		Assert.assertEquals(3, s1.estimateDistinct());

	}

	@Test
	public void testCountDistanceEqual() {
		LinearCountingSketch s1 = new LinearCountingSketch();
		LinearCountingSketch s2 = new LinearCountingSketch();

		s1.add("d1");
		s1.add("d1");
		s1.add("d2");

		s2.add("d2");
		s2.add("d2");
		s2.add("d1");
		Assert.assertEquals(0.0, s1.computeJaccardDistanceTo(s2));

	}

	@Test
	public void testCountDistanceDisjoint() {
		LinearCountingSketch s1 = new LinearCountingSketch();
		LinearCountingSketch s2 = new LinearCountingSketch();

		s1.add("d1");
		s1.add("d1");
		s1.add("d2");

		s2.add("d3");
		s2.add("d3");
		s2.add("d4");
		Assert.assertEquals(1.0, s1.computeJaccardDistanceTo(s2));

	}

	@Test
	public void testCountDistanceIntersect() {
		LinearCountingSketch s1 = new LinearCountingSketch();
		LinearCountingSketch s2 = new LinearCountingSketch();

		s1.add("d1");
		s1.add("d2");
		s1.add("d3");
		s1.add("d4");

		s2.add("d3");
		s2.add("d4");
		s2.add("d5");
		Assert.assertEquals(0.6, s1.computeJaccardDistanceTo(s2));

	}
}
