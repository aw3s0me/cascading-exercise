package de.fraunhofer.iais.kd.livlab.bda.clustermodel;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import de.fhg.iais.kd.hadoop.recommender.userset.UserSet;

public class ClusterModel implements Serializable {

	private static final long serialVersionUID = 1L;
	private final Map<String, String> map = new HashMap<String, String>();

	public void put(String url, String tag) {
		map.put(url, tag);
	}

	public String get(String url) {
		return map.get(url);
	}

	public Set<String> getKeys() {
		return map.keySet();
	}

	/**
	 * 
	 * @param clusterId
	 * @return
	 */
	public UserSet getValue(String clusterId) {
		String value = this.get(clusterId);
		UserSet userIds = new UserSet();
		// Values Example: 0 1 0 1 0 1
		if (!value.isEmpty()) {
			String[] metroid = value.split(" ");
			// Get user ids to use as jaccard distance
			for (int i = 0; i < metroid.length; i++) {
				if (metroid[i].contains("1")) {
					userIds.add(Integer.toString(i));
				}
			}

			return userIds;
		}
		return null;
	}

}
