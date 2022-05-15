package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.function.SerializableFunction;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 *  当前ReducingUpsertSink类由default改为public
 */

public class ReducingUpsertSink<WriterState> implements Sink<RowData, Void, WriterState, Void>  {
    private final Sink<RowData, ?, WriterState, ?> wrappedSink;
    private final DataType physicalDataType;
    private final int[] keyProjection;
    private final SinkBufferFlushMode bufferFlushMode;
    private final SerializableFunction<RowData, RowData> valueCopyFunction;

    public ReducingUpsertSink(
            Sink<RowData, ?, WriterState, ?> wrappedSink,
            DataType physicalDataType,
            int[] keyProjection,
            SinkBufferFlushMode bufferFlushMode,
            SerializableFunction<RowData, RowData> valueCopyFunction) {
        this.wrappedSink = wrappedSink;
        this.physicalDataType = physicalDataType;
        this.keyProjection = keyProjection;
        this.bufferFlushMode = bufferFlushMode;
        this.valueCopyFunction = valueCopyFunction;
    }

    @Override
    public SinkWriter<RowData, Void, WriterState> createWriter(
            Sink.InitContext context, List<WriterState> states) throws IOException {
        final SinkWriter<RowData, ?, WriterState> wrapperWriter =
                wrappedSink.createWriter(context, states);
        return new ReducingUpsertWriter<>(
                wrapperWriter,
                physicalDataType,
                keyProjection,
                bufferFlushMode,
                context.getProcessingTimeService(),
                valueCopyFunction);
    }

    @Override
    public Optional<SimpleVersionedSerializer<WriterState>> getWriterStateSerializer() {
        return wrappedSink.getWriterStateSerializer();
    }

    @Override
    public Optional<Committer<Void>> createCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<Void, Void>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }
}
