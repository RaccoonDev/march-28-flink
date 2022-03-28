/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.devraccoon;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchJob {

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


		DataSource<String> stringDataSource = env.readTextFile("data/words.txt");
		/*
			element1 > sam said something
			element2 > something else happened
		 */
		stringDataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			@Override
			public void flatMap(String element, Collector<Tuple2<String, Integer>> collector) throws Exception {
				String[] words = element.split(" ");
				for(String word: words) {
					collector.collect(Tuple2.of(word, 1));
				}
			}
		}).groupBy(0).sum(1).print();
		/*
			element1 > (sam, 1)
			element2 > (said, 1)
			element3 > (something, 1)
			element4 > (something, 1)
			element5 > (else, 1)
			...
		 */

		env.execute("Flink Batch Java API Skeleton");
	}
}
