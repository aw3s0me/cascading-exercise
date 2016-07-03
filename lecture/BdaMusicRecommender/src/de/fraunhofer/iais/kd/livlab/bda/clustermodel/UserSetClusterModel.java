package de.fraunhofer.iais.kd.livlab.bda.clustermodel;

import java.util.Set;

import de.fhg.iais.kd.hadoop.recommender.userset.UserSet;

public class UserSetClusterModel {

	public ClusterModel model;

	public UserSetClusterModel(ClusterModel model) {
		// TODO Auto-generated constructor stub
		this.model = model;
	}

	/**
	 * calculate closest metroid min of square distance to other points in
	 * cluster Based on Jaccard Distances (in matrix)
	 * 
	 * @param set
	 * @return
	 */
	public String findClosestCluster(UserSet set) {
		// get cluster keys (cluster ids)
		Set<String> keys = this.model.getKeys();
		String closest = "";
		// max distance of different sets is 1
		double minDist = 1.0;

		for (String cluster : keys) {
			// Format is 0,1,0,1,0 of metroid. Transform to UserSet with userIds
			UserSet clusterSet = this.model.getValue(cluster);
			// Jaccard Distance of userIds Sets
			double dist = set.distanceTo(clusterSet);

			if (dist <= minDist) {
				minDist = dist;
				closest = cluster;
			}
		}

		return closest;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
