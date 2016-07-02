package de.fraunhofer.iais.kd.livlab.bda.clustermodel;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
	public String[] getValue(String clusterId) {
		String value = this.get(clusterId);

		// Values Example: 0 1 0 1 0 1
		if (!value.isEmpty()) {

			return value.split(" ");
		}
		return null;
	}

}
