package de.fraunhofer.iais.kd.livlab.bda.countdistinct.detector;

import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModel;
import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModelFactory;
import de.fraunhofer.iais.kd.livlab.bda.config.BdaConstants;

/**
 * @author mmock Count Distinct
 */

public class CountDistinctDetecor {

	private final int count = 0;
	private final ClusterModel model;

	public CountDistinctDetecor() {
		model = ClusterModelFactory
				.readFromCsvResource(BdaConstants.CLUSTER_MODEL);

	}

	public String[] process(String userid, String artid, String artname) {

		String[] result = new String[2];

		result[0] = userid;
		result[1] = artname;

		return result;
	}

}
