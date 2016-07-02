package de.fhg.iais.kd.hadoop.recommender.userset;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.lang3.StringUtils;

/**
 * Implements a class UserSet to store a user names set
 * 
 * @author akorovin
 */
public class UserSet {

	public Set<String> users;

	public UserSet() {
		this.users = new HashSet<String>();
	}

	public UserSet(String[] users) {
		this.users = new HashSet<String>(Arrays.asList(users));
	}

	/**
	 * Adds a user to the userset
	 * 
	 * @param user
	 */
	public void add(String user) {
		users.add(user);
	}

	/**
	 * Calculates Jaccard Distance to another userset
	 * 
	 * @param other
	 * @return
	 */
	public double distanceTo(UserSet other) {
		Set<String> union = new HashSet<String>(this.users);
		union.addAll(other.users);

		Set<String> intersection = new HashSet<String>(this.users);
		intersection.retainAll(other.users);

		return 1.0 - intersection.size() / (double) union.size();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		UserSet userSet1 = new UserSet(new String[] { "1", "2", "5" });
		UserSet userSet2 = new UserSet(new String[] { "1", "2", "3", "4" });

		double jaccardDist = userSet1.distanceTo(userSet2);
		System.out.println("Jaccard Distance from UserSet: " + jaccardDist);
		Assert.assertEquals(0.6, jaccardDist);
	}

	/**
	 * transform user set to fit to needed format. Otherwise it will look like:
	 * 4Hero|de.fhg.iais....UserSet@6ccead66
	 */
	@Override
	public String toString() {
		return StringUtils.join(this.users, ',');
	}
}
