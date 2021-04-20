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

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.input.UnionStateInputFormat;
import org.apache.flink.state.api.runtime.BootstrapTransformationWithID;
import org.apache.flink.state.api.runtime.metadata.SavepointMetadata;
import org.apache.flink.streaming.connectors.pulsar.internal.TopicSubscription;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.client.api.MessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * flink state migrate tool main entry.
 */
public class MigrateStateTool {

    private static final Logger log = LoggerFactory.getLogger(MigrateStateTool.class);

    public static void main(String[] args) throws Exception {
        final ParameterTool tool = ParameterTool.fromArgs(args);
        if (tool.has("help")) {
            printHelp();
        }
        if (!tool.has("savepointPath")) {
            printHelp();
        }
        if (!tool.has("newStatePath")) {
            printHelp();
        }
        final String savepointPath = tool.get("savepointPath");
        final String newStatePath = tool.get("newStatePath");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ExistingSavepoint existingSavepoint = Savepoint.load(env, savepointPath, new MemoryStateBackend());
        SavepointMetadata metadata = (SavepointMetadata) FieldUtils.readDeclaredField(existingSavepoint, "metadata", true);
        Map<OperatorID, Object> operatorStateIndex = (Map<OperatorID, Object>) FieldUtils.readDeclaredField(metadata, "operatorStateIndex", true);
        final List<OperatorState> existingOperators = metadata.getExistingOperators();
        for (OperatorState operator : existingOperators) {
            final Optional<BootstrapTransformation<Tuple2<TopicSubscription, MessageId>>> transform = getNewStateTransformation(operator, env);
            if (!transform.isPresent()) {
                log.info("operatorState [{}] not exist pulsar state", operator.getOperatorID());
                continue;
            }
            final BootstrapTransformationWithID transformation = new BootstrapTransformationWithID(operator.getOperatorID(), transform.get());
            final Object newInstance = getNewOperatorStateSpec(transformation);
            operatorStateIndex.remove(operator.getOperatorID());
            operatorStateIndex.put(operator.getOperatorID(), newInstance);
        }
        existingSavepoint
                .write(newStatePath);
        env.execute();
    }

    private static Object getNewOperatorStateSpec(BootstrapTransformationWithID transformation) throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException {
        Class<?> cls = SavepointMetadata.class.getClassLoader().loadClass("org.apache.flink.state.api.runtime.metadata.OperatorStateSpec");
        final Constructor<?> constructor = cls.getDeclaredConstructor(BootstrapTransformationWithID.class);
        constructor.setAccessible(true);
        return constructor.newInstance(transformation);
    }

    private static Optional<BootstrapTransformation<Tuple2<TopicSubscription, MessageId>>> getNewStateTransformation(OperatorState operatorState, ExecutionEnvironment env) throws Exception {
        final TypeInformation<Tuple2<String, MessageId>> typeInfo = TypeInformation.of(new TypeHint<Tuple2<String, MessageId>>() {
        });
        final DataSet<Tuple2<String, MessageId>> tuple2DataSet = readUnionState(SimpleBootstrapFunction.OFFSETS_STATE_NAME, operatorState, env, typeInfo);
        final List<Tuple2<String, MessageId>> collect = tuple2DataSet.collect();
        if (collect.isEmpty()) {
            return Optional.empty();
        }
        log.info("operatorState [{}] exist pulsar state {}", operatorState.getOperatorID(), collect);
        final DataSet<String> subName = readUnionState(SimpleBootstrapFunction.OFFSETS_STATE_NAME + "_subName", operatorState, env, TypeInformation.of(String.class));
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
        return Optional.of(transform);
    }

    private static <T> DataSet<T> readUnionState(String name, OperatorState operatorState, ExecutionEnvironment env, TypeInformation<T> typeInfo) {
        ListStateDescriptor<T> descriptor = new ListStateDescriptor<>(name, typeInfo);
        UnionStateInputFormat<T> inputFormat = new UnionStateInputFormat<>(operatorState, descriptor);
        return env.createInput(inputFormat, typeInfo);
    }

    private static void printHelp() {
        System.out.println("Usage:");
        System.out.println("\t-savepointPath      flink savepoint path");
        System.out.println("\t-newStatePath      new flink savepoint path");
        System.exit(-1);
    }

}
