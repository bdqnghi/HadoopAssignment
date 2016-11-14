/**
 * Copyright 2012 Jee Vang 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0 
 *  
 *  Unless required by applicable law or agreed to in writing, software 
 *  distributed under the License is distributed on an "AS IS" BASIS, 
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 *  See the License for the specific language governing permissions and 
 *  limitations under the License. 
 */
package com.hadoop.assignment.question1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Groups values based on symbol of {@link UserLocationKey} (the natural key).
 * @author Jee Vang
 *
 */
public class NaturalKeyGroupingComparator extends WritableComparator {

	/**
	 * Constructor.
	 */
	protected NaturalKeyGroupingComparator() {
		super(UserLocationKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		UserLocationKey k1 = (UserLocationKey)w1;
		UserLocationKey k2 = (UserLocationKey)w2;
		
		return k1.getUserId().compareTo(k2.getUserId());
	}
}
