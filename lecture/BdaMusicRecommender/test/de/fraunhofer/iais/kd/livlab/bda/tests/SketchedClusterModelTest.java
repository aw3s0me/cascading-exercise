package de.fraunhofer.iais.kd.livlab.bda.tests;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModel;
import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModelFactory;
import de.fraunhofer.iais.kd.livlab.bda.countdistinct.LinearCountingSketch;
import de.fraunhofer.iais.kd.livlab.bda.countdistinct.SketchedClusterModel;

public class SketchedClusterModelTest {
	@Test
	public void testCreateFromModel() {
		ClusterModel model = ClusterModelFactory
				.readFromCsvResource("resources/centers_1000_iter_2_50.csv");

		SketchedClusterModel sketchedModel = new SketchedClusterModel(model);
		Assert.assertNotNull(sketchedModel);
	}

	@Test
	public void testGetClosest() {
		LinearCountingSketch s1 = new LinearCountingSketch();
		LinearCountingSketch s2 = new LinearCountingSketch();
		LinearCountingSketch s3 = new LinearCountingSketch();
		Map<String, LinearCountingSketch> map = new HashMap<String, LinearCountingSketch>();

		s1.add("d1");
		s1.add("d2");
		s1.add("d3");
		s1.add("d4");

		s2.add("d3");
		s2.add("d4");
		s2.add("d5");
		s2.add("d6");

		s3.add("d4");
		s3.add("d5");
		s3.add("d6");
		s3.add("d7");

		map.put("s1", s1);
		map.put("s2", s2);

		SketchedClusterModel smodel = new SketchedClusterModel(map);

		String closest = smodel.findClosestCluster(s3);
		Assert.assertEquals("s2", closest);
	}

	@Test
	public void testGetClosestFromObjectmodel() {
		LinearCountingSketch s1 = new LinearCountingSketch();

		s1.add("user_001");
		s1.add("user_002");
		s1.add("user_003");
		s1.add("user_004");

		SketchedClusterModel smodel = new SketchedClusterModel(
				ClusterModelFactory
						.readFromCsvResource("resources/centers_1000_iter_2_50.csv"));

		String closest = smodel.findClosestCluster(s1);
		Assert.assertTrue(!closest.isEmpty());
	}

	@Test
	public void testGetClosestForUser_000001l() {
		LinearCountingSketch s1 = new LinearCountingSketch();

		s1.add("user_000001");

		SketchedClusterModel smodel = new SketchedClusterModel(
				ClusterModelFactory
						.readFromCsvResource("resources/centers_1000_iter_2_50.csv"));

		String closest = smodel.findClosestCluster(s1);
		Assert.assertEquals("\"31\"", closest);
	}

	@Test
	public void testGetClosestForUser_000075l() {
		LinearCountingSketch s1 = new LinearCountingSketch();

		s1.add("user_000074");

		SketchedClusterModel smodel = new SketchedClusterModel(
				ClusterModelFactory
						.readFromCsvResource("resources/centers_1000_iter_2_50.csv"));

		String closest = smodel.findClosestCluster(s1);
		Assert.assertEquals("\"34\"", closest);
	}
}
