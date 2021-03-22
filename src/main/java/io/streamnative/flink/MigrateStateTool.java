/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.streamnative.flink;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.streaming.connectors.pulsar.internal.TopicSubscription;

import org.apache.pulsar.client.api.MessageId;

import java.util.List;

/**
 * flink state migrate tool main entry.
 */
public class MigrateStateTool {

    public static void main(String[] args) throws Exception {
        final ParameterTool tool = ParameterTool.fromArgs(args);
        if (tool.has("help")) {
            printHelp();
        }
        if (!tool.has("savepointPath")) {
            printHelp();
        }
        if (!tool.has("uid")) {
            printHelp();
        }
        if (!tool.has("newStatePath")) {
            printHelp();
        }
        final String uid = tool.get("uid", "pulsar-source-id");
        final String savepointPath = tool.get("savepointPath");
        final String newStatePath = tool.get("newStatePath");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ExistingSavepoint existingSavepoint = Savepoint.load(env, savepointPath, new MemoryStateBackend());

        final DataSet<Tuple2<String, MessageId>> tuple2DataSet = existingSavepoint.readUnionState(uid, SimpleBootstrapFunction.OFFSETS_STATE_NAME, TypeInformation.of(new TypeHint<Tuple2<String, MessageId>>() {
        }));
        tuple2DataSet.print();
        final DataSet<String> subName = existingSavepoint.readUnionState(uid, SimpleBootstrapFunction.OFFSETS_STATE_NAME + "_subName", TypeInformation.of(String.class));
        subName.print();
        final List<String> subNames = subName.collect();

        final SimpleBootstrapFunction processFunction = new SimpleBootstrapFunction();

        DataSet<Tuple2<TopicSubscription, MessageId>> result;

        if (subNames.isEmpty()) {
            result = tuple2DataSet.map(tuple2 -> Tuple2.of(TopicSubscription.builder().topic(tuple2.f0).build(), tuple2.f1))
                    .returns(processFunction.getProducedType());
        } else {
            result = tuple2DataSet.cross(subName)
                    .with((tuple2, sub) -> Tuple2.of(TopicSubscription.builder().topic(tuple2.f0).subscriptionName(sub).build(), tuple2.f1))
                    .returns(processFunction.getProducedType());
        }
        final BootstrapTransformation<Tuple2<TopicSubscription, MessageId>> transform = OperatorTransformation.bootstrapWith(result)
                .transform(processFunction);

        existingSavepoint
                .removeOperator(uid)
                .withOperator(uid, transform)
                .write(newStatePath);
        env.execute();
    }

    private static void printHelp() {
        System.out.println("Usage:");
        System.out.println("\t-uid                pulsar source uid");
        System.out.println("\t-savepointPath      flink savepoint path");
        System.out.println("\t-newStatePath      new flink savepoint path");
        System.exit(-1);
    }

}
