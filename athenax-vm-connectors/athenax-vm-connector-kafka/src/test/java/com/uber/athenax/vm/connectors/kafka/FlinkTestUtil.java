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

package com.uber.athenax.vm.connectors.kafka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

final class FlinkTestUtil {

  private FlinkTestUtil() {
  }

  static MiniCluster execute(LocalStreamEnvironment env,
                             Configuration conf, String jobName) throws Exception {
    StreamGraph streamGraph = env.getStreamGraph();
    streamGraph.setJobName(jobName);
    JobGraph jobGraph = streamGraph.getJobGraph();
    Configuration configuration = new Configuration(conf);
    configuration.addAll(jobGraph.getJobConfiguration());
    configuration.setLong("taskmanager.memory.size", -1L);
    configuration.setInteger("taskmanager.numberOfTaskSlots", jobGraph.getMaximumParallelism());
    MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration(
        configuration, 1, RpcServiceSharing.SHARED, "localhost:8888"
    );

    MiniCluster cluster = new MiniCluster(miniClusterConfiguration);
    cluster.start();
    cluster.submitJob(jobGraph);
    return cluster;
  }
}
