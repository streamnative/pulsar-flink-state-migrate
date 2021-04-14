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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.state.api.functions.StateBootstrapFunction;
import org.apache.flink.streaming.connectors.pulsar.internal.MessageIdSerializer;
import org.apache.flink.streaming.connectors.pulsar.internal.TopicSubscription;
import org.apache.flink.streaming.connectors.pulsar.internal.TopicSubscriptionSerializer;

import org.apache.pulsar.client.api.MessageId;

/**
 * update pulsar source function.
 */
public class SimpleBootstrapFunction extends StateBootstrapFunction<Tuple2<TopicSubscription, MessageId>> implements ResultTypeQueryable<Tuple2<TopicSubscription, MessageId>> {

    public static final String OFFSETS_STATE_NAME = "topic-partition-offset-states";
    public static final String NEW_OFFSETS_STATE_NAME = "topic-offset-states";

    private ListState<Tuple2<TopicSubscription, MessageId>> state;

    private final TypeInformation<Tuple2<TopicSubscription, MessageId>> typeInformation = TypeInformation.of(new TypeHint<Tuple2<TopicSubscription, MessageId>>() {
    });

    @Override
    public void processElement(Tuple2<TopicSubscription, MessageId> value, Context ctx) throws Exception {
        System.out.println("state processElement" + value);
        state.add(value);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        TypeSerializer<?>[] fieldSerializers =
				new TypeSerializer<?>[]{
						TopicSubscriptionSerializer.INSTANCE,
						MessageIdSerializer.INSTANCE
				};
		@SuppressWarnings("unchecked")
		Class<Tuple2<TopicSubscription, MessageId>> tupleClass =
				(Class<Tuple2<TopicSubscription, MessageId>>) (Class<?>) Tuple2.class;
		TupleSerializer<Tuple2<TopicSubscription, MessageId>> tuple2TupleSerializer = new TupleSerializer<>(tupleClass, fieldSerializers);

        state = context.getOperatorStateStore().getUnionListState(
                new ListStateDescriptor<>(
                        NEW_OFFSETS_STATE_NAME,
                        tuple2TupleSerializer));
    }

    @Override
    public TypeInformation<Tuple2<TopicSubscription, MessageId>> getProducedType() {
        return typeInformation;
    }
}
