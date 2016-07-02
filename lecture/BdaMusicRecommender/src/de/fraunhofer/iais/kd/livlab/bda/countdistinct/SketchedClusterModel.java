package de.fraunhofer.iais.kd.livlab.bda.countdistinct;

import java.util.HashMap;
import java.util.Map;

import de.fraunhofer.iais.kd.livlab.bda.clustermodel.ClusterModel;

public class SketchedClusterModel {

	private final Map<String, LinearCountingSketch> map;

	public SketchedClusterModel(ClusterModel model) {

		map = new HashMap<String, LinearCountingSketch>();

		for (String clusterid : model.getKeys()) {
			LinearCountingSketch sketch = new LinearCountingSketch();
			fillSketchFromModelString(sketch, model.get(clusterid));
			map.put(clusterid, sketch);

		}

	}

	public SketchedClusterModel(Map<String, LinearCountingSketch> map) {

		this.map = map;

	}

	public String findClosestCluster(LinearCountingSketch sketch) {

		String found = "";
		double mindistance = 2.0;
		for (String clusterid : map.keySet()) {
			LinearCountingSketch metroid = map.get(clusterid);
			double distance = sketch.computeJaccardDistanceTo(metroid);
			if (distance < mindistance) {
				mindistance = distance;
				found = clusterid;
			}
		}
		return found;
	}

	private void fillSketchFromModelString(LinearCountingSketch sketch,
			String userarray) {

		if (userarray.length() == 2001) { // csv model 0,0,1,1, ... for 1000
											// users (including user_000)

			for (int id = 1; id <= 1000; id++) {

				if (userarray.charAt(2 * id) == '1') {
					String uid = "user_" + String.format("%06d", id); // transform
																		// id 1
																		// to
																		// user_001
																		// etc.
					sketch.add(uid);
				}
			}

		}
	}
}
