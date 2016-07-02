package de.fraunhofer.iais.kd.livlab.bda.tests;

import junit.framework.Assert;

import org.junit.Test;

import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModel;
import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModelFactory;

public class ClusterModelFactoryTest {

	@Test
	public void modelCreateTest() {
		ClusterModel model = ClusterModelFactory
				.readFromCsvResource("resources/centers_1000_iter_2_50.csv");
		Assert.assertNotNull(model);
	}

}
