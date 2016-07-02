package de.fraunhofer.iais.kd.livlab.bda.countdistinct;

import java.io.Serializable;
import java.util.BitSet;

public class Sketch implements Serializable {

	private static final long serialVersionUID = -754452134281626194L;

	private BitSet sketch;
	private int sketchsize;

	public Sketch(int sketchsize) {

		this.sketchsize = sketchsize;
		sketch = new BitSet();
		// this.sketch = new BitSet(sketchsize);

	}

	public void setBit(int index) {
		sketch.set(index);
	}

	public boolean getBit(int index) {
		return sketch.get(index);
	}

	public BitSet getSketch() {
		return sketch;
	}

	public void setSketch(BitSet sketch) {
		this.sketch = sketch;
	}

	public int getSketchsize() {
		return sketchsize;
	}

	public void setSketchsize(int sketchsize) {
		this.sketchsize = sketchsize;
	}

	public Sketch orSketch(Sketch sketch) {

		(this.sketch).or(sketch.getSketch());

		Sketch newSketch = new Sketch(sketchsize);
		newSketch.setSketch(this.sketch);

		return newSketch;

	}

	public void clear() {

		sketch.clear();
		sketchsize = 0;

	}

	public Sketch copy() {

		Sketch sketchCopy = new Sketch(sketchsize);
		sketchCopy.setSketch((BitSet) (sketch.clone()));

		return sketchCopy;

	}

	@Override
	public String toString() {
		return ("Sketch [sketchsize:" + sketchsize + ", " + sketch.toString() + "]");
	}

	public int cardinality() {
		return sketch.cardinality();
	}

	public void mergeWidth(Sketch s) {
		sketch.or(s.sketch);

	}

}
