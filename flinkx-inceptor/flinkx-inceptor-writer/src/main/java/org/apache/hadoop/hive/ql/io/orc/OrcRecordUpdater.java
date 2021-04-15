/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.io.orc;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.persistence.AbstractRowContainer.RowIterator;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat.Options;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.WriterCallback;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.WriterContext;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.WriterOptions;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct.Field;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct.OrcStructInspector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class OrcRecordUpdater implements RecordUpdater {
    private static final Log LOG = LogFactory.getLog(OrcRecordUpdater.class);
    public static final String ACID_KEY_INDEX_NAME = "hive.acid.key.index";
    public static final String ACID_FORMAT = "_orc_acid_version";
    public static final String ACID_STATS = "hive.acid.stats";
    public static final String BF_COLUMNS = "orc.bloomfilter.columns";
    public static final String COLUMN_INDEX_CONCAT_STR = "orc.column.index.concat.str";
    public static final int ORC_ACID_VERSION = 0;
    static final int INSERT_OPERATION = 0;
    static final int UPDATE_OPERATION = 1;
    static final int DELETE_OPERATION = 2;
    static final int OPERATION = 0;
    static final int ORIGINAL_TRANSACTION = 1;
    static final int BUCKET = 2;
    static final int ROW_ID = 3;
    static final int CURRENT_TRANSACTION = 4;
    static final int ROW = 5;
    static final int FIELDS = 6;
    static final int DELTA_BUFFER_SIZE = 16384;
    static final long DELTA_STRIPE_SIZE = 16777216L;
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private final Options options;
    private final Path path;
    private final FileSystem fs;
    private Writer writer;
    private final FSDataOutputStream flushLengths;
    private final OrcStruct item;
    private final IntWritable operation = new IntWritable();
    private final LongWritable currentTransaction = new LongWritable(-1L);
    private final LongWritable originalTransaction = new LongWritable(-1L);
    private final IntWritable bucket = new IntWritable();
    private final LongWritable rowId = new LongWritable();
    private long insertedRows = 0L;
    private long rowCountDelta = 0L;
    private final KeyIndexBuilder indexBuilder = new KeyIndexBuilder();
    private StructField recIdField = null;
    private StructField rowIdField = null;
    private StructField originalTxnField = null;
    private StructObjectInspector rowInspector;
    private StructObjectInspector recIdInspector;
    private LongObjectInspector rowIdInspector;
    private LongObjectInspector origTxnInspector;
    private RowContainer<List<Object>> pendingInsertRowsForMergeInto = null;
    private long mergeInsertTransactionId = -1L;
    private static final Charset utf8 = Charset.forName("UTF-8");

    static Path getSideFile(Path main) {
        return new Path(main + "_flush_length");
    }

    static int getOperation(OrcStruct struct) {
        return ((IntWritable)struct.getFieldValue(0)).get();
    }

    public static long getCurrentTransaction(OrcStruct struct) {
        return ((LongWritable)struct.getFieldValue(4)).get();
    }

    static long getOriginalTransaction(OrcStruct struct) {
        return ((LongWritable)struct.getFieldValue(1)).get();
    }

    static int getBucket(OrcStruct struct) {
        return ((IntWritable)struct.getFieldValue(2)).get();
    }

    static long getRowId(OrcStruct struct) {
        return ((LongWritable)struct.getFieldValue(3)).get();
    }

    static OrcStruct getRow(OrcStruct struct) {
        return struct == null ? null : (OrcStruct)struct.getFieldValue(5);
    }

    static StructObjectInspector createEventObjectInspector(ObjectInspector rowInspector) {
        List<StructField> fields = new ArrayList();
        fields.add(new Field("operation", PrimitiveObjectInspectorFactory.writableIntObjectInspector, 0, "", (String)null, false, false, false));
        fields.add(new Field("originalTransaction", PrimitiveObjectInspectorFactory.writableLongObjectInspector, 1, "", (String)null, false, false, false));
        fields.add(new Field("bucket", PrimitiveObjectInspectorFactory.writableIntObjectInspector, 2, "", (String)null, false, false, false));
        fields.add(new Field("rowId", PrimitiveObjectInspectorFactory.writableLongObjectInspector, 3, "", (String)null, false, false, false));
        fields.add(new Field("currentTransaction", PrimitiveObjectInspectorFactory.writableLongObjectInspector, 4, "", (String)null, false, false, false));
        fields.add(new Field("row", rowInspector, 5, "", (String)null, false, false, false));
        return new OrcStructInspector(fields);
    }

    public static TypeDescription createEventSchema(TypeDescription typeDescr) {
        TypeDescription result = TypeDescription.createStruct().addField("operation", TypeDescription.createInt()).addField("originalTransaction", TypeDescription.createLong()).addField("bucket", TypeDescription.createInt()).addField("rowId", TypeDescription.createLong()).addField("currentTransaction", TypeDescription.createLong()).addField("row", typeDescr.clone());
        return result;
    }

    private static TypeDescription createEventSchemaFromTableProperties(Properties tableProps) {
        TypeDescription rowSchema = OrcOutputFormat.getSchemaSettingFromProps(tableProps);
        return rowSchema == null ? null : createEventSchema(rowSchema);
    }

    OrcRecordUpdater(Path path, Options options) throws HiveException {
        try {
            this.options = options;
            this.bucket.set(options.getBucket());
            this.path = AcidUtils.createFilename(path, options);
            FileSystem fs = options.getFilesystem();
            if (fs == null) {
                fs = path.getFileSystem(options.getConfiguration());
            }

            this.fs = fs;

            try {
                FSDataOutputStream strm = fs.create(new Path(path, "_orc_acid_version"), false);
                strm.writeInt(0);
                strm.close();
            } catch (IOException var6) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Failed to create " + path + "/" + "_orc_acid_version" + " with " + var6);
                }
            }

            this.flushLengths = fs.create(getSideFile(this.path), true, 8, options.getReporter());

            WriterOptions writerOptions = null;
            if (options instanceof OrcOptions) {
                writerOptions = ((OrcOptions)options).getOrcOptions();
            }

            if (writerOptions == null) {
                writerOptions = OrcFile.writerOptions(options.getConfiguration());
            }

            writerOptions.fileSystem(fs).callback(this.indexBuilder);
            if (!options.isWritingBase()) {
                writerOptions.blockPadding(false);
                writerOptions.bufferSize(16384);
                writerOptions.stripeSize(16777216L);
                if (!HiveConf.getBoolVar(writerOptions.getConfiguration(), ConfVars.NGMR_MIN_MAX_FILTER_TORC_ALLOWED)) {
                    writerOptions.rowIndexStride(0);
                }
            }

            this.rowInspector = (StructObjectInspector)options.getInspector();
            writerOptions.inspector(createEventObjectInspector(this.findRecId(options.getInspector(), options.getRecordIdColumn())));
            writerOptions.setSchema(createEventSchemaFromTableProperties(options.getTableProperties()));
            writerOptions.isAcidWriter(true);
            this.writer = OrcFile.createWriter(this.path, writerOptions);
            this.item = new OrcStruct(6);
            this.item.setFieldValue(0, this.operation);
            this.item.setFieldValue(4, this.currentTransaction);
            this.item.setFieldValue(1, this.originalTransaction);
            this.item.setFieldValue(2, this.bucket);
            this.item.setFieldValue(3, this.rowId);
            int onHeapRows = HiveConf.getIntVar(writerOptions.getConfiguration(), ConfVars.ORC_MERGE_INSERT_BUFFER_SIZE);
            if (options.isRowContainerRequired()) {
                this.pendingInsertRowsForMergeInto = AcidUtils.getRowContainer(writerOptions.getConfiguration(), this.rowInspector, onHeapRows, options.getReporter());
            } else {
                this.pendingInsertRowsForMergeInto = null;
            }

        } catch (HiveException var7) {
            throw var7;
        } catch (Exception var8) {
            throw new HiveException(var8);
        }
    }

    private ObjectInspector findRecId(ObjectInspector inspector, int rowIdColNum) {
        if (!(inspector instanceof StructObjectInspector)) {
            throw new RuntimeException("Serious problem, expected a StructObjectInspector, but got a " + inspector.getClass().getName());
        } else if (rowIdColNum < 0) {
            return inspector;
        } else {
            RecIdStrippingObjectInspector newInspector = new RecIdStrippingObjectInspector(inspector, rowIdColNum);
            this.recIdField = newInspector.getRecId();
            List<? extends StructField> fields = ((StructObjectInspector)this.recIdField.getFieldObjectInspector()).getAllStructFieldRefs();
            this.originalTxnField = (StructField)fields.get(0);
            this.origTxnInspector = (LongObjectInspector)this.originalTxnField.getFieldObjectInspector();
            this.rowIdField = (StructField)fields.get(2);
            this.rowIdInspector = (LongObjectInspector)this.rowIdField.getFieldObjectInspector();
            this.recIdInspector = (StructObjectInspector)this.recIdField.getFieldObjectInspector();
            return newInspector;
        }
    }

    private void addEvent(int operation, long currentTransaction, long rowId, Object row) throws IOException {
        this.operation.set(operation);
        this.currentTransaction.set(currentTransaction);
        long originalTransaction = currentTransaction;
        if (operation == 2 || operation == 1) {
            Object rowIdValue = this.rowInspector.getStructFieldData(row, this.recIdField);
            originalTransaction = this.origTxnInspector.getLong(this.recIdInspector.getStructFieldData(rowIdValue, this.originalTxnField));
            rowId = this.rowIdInspector.getLong(this.recIdInspector.getStructFieldData(rowIdValue, this.rowIdField));
        }

        this.rowId.set(rowId);
        this.originalTransaction.set(originalTransaction);
        this.item.setFieldValue(5, operation == 2 ? null : row);
        this.indexBuilder.addKey(operation, originalTransaction, this.bucket.get(), rowId);
        this.writer.addRow(this.item);
    }

    public void insert(long currentTransaction, Object row) throws IOException {
        if (this.currentTransaction.get() != currentTransaction) {
            this.insertedRows = 0L;
        }

        this.addEvent(0, currentTransaction, (long)(this.insertedRows++), row);
        ++this.rowCountDelta;
    }

    public void mergeInsert(long currentTransaction, Object row) throws HiveException {
        this.mergeInsertTransactionId = currentTransaction;
        List<Object> insertRow = new ArrayList();
        if (row instanceof Object[]) {
            for(int i = 0; i < ((Object[])((Object[])row)).length; ++i) {
                insertRow.add(((Object[])((Object[])row))[i]);
            }
        } else {
            insertRow.add(row);
        }

        if (this.pendingInsertRowsForMergeInto != null) {
            this.pendingInsertRowsForMergeInto.addRow(insertRow);
        }

    }

    public void update(long currentTransaction, Object row) throws IOException {
        if (this.currentTransaction.get() != currentTransaction) {
            this.insertedRows = 0L;
        }

        this.addEvent(1, currentTransaction, -1L, row);
    }

    public void delete(long currentTransaction, Object row) throws IOException {
        if (this.currentTransaction.get() != currentTransaction) {
            this.insertedRows = 0L;
        }

        this.addEvent(2, currentTransaction, -1L, row);
        --this.rowCountDelta;
    }

    public void flush() throws IOException {
        if (this.flushLengths == null) {
            throw new IllegalStateException("Attempting to flush a RecordUpdater on " + this.path + " with a single transaction.");
        } else {
            long len = this.writer.writeIntermediateFooter();
            this.flushLengths.writeLong(len);
            OrcInputFormat.SHIMS.hflush(this.flushLengths);
        }
    }

    private void writePendingInsertRows() throws HiveException {
        if (this.pendingInsertRowsForMergeInto != null) {
            try {
                RowIterator<List<Object>> iter = this.pendingInsertRowsForMergeInto.rowIter();

                for(List rightObj = (List)iter.first(); rightObj != null; rightObj = (List)iter.next()) {
                    if (this.currentTransaction.get() != this.mergeInsertTransactionId) {
                        this.insertedRows = 0L;
                    }

                    this.addEvent(0, this.mergeInsertTransactionId, (long)(this.insertedRows++), rightObj);
                    ++this.rowCountDelta;
                }
            } catch (HiveException var8) {
                throw var8;
            } catch (Exception var9) {
                throw new HiveException(var9);
            } finally {
                this.pendingInsertRowsForMergeInto.clearRows();
            }

        }
    }

    public void close(boolean abort) throws IOException {
        if (abort) {
            if (this.flushLengths == null) {
                this.fs.delete(this.path, false);
            }
        } else {
            try {
                this.writePendingInsertRows();
            } catch (Exception var3) {
                throw new IOException(var3.toString());
            }

            if (this.writer != null) {
                this.writer.close();
            }
        }

        if (this.flushLengths != null) {
            this.flushLengths.close();
            this.fs.delete(getSideFile(this.path), false);
        }

        this.writer = null;
    }

    public SerDeStats getStats() {
        SerDeStats stats = new SerDeStats();
        stats.setRowCount(this.rowCountDelta);
        return stats;
    }

    @VisibleForTesting
    Writer getWriter() {
        return this.writer;
    }

    static AcidStats parseAcidStats(Reader reader) {
        String statsSerialized;
        try {
            ByteBuffer val = reader.getMetadataValue("hive.acid.stats").duplicate();
            statsSerialized = utf8.newDecoder().decode(val).toString();
        } catch (CharacterCodingException var3) {
            throw new IllegalArgumentException("Bad string encoding for hive.acid.stats", var3);
        }

        return new AcidStats(statsSerialized);
    }

    static RecordIdentifier[] parseKeyIndex(Reader reader) {
        String[] stripes;
        try {
            ByteBuffer val = reader.getMetadataValue("hive.acid.key.index").duplicate();
            stripes = utf8.newDecoder().decode(val).toString().split(";");
        } catch (CharacterCodingException var5) {
            throw new IllegalArgumentException("Bad string encoding for hive.acid.key.index", var5);
        }

        RecordIdentifier[] result = new RecordIdentifier[stripes.length];

        for(int i = 0; i < stripes.length; ++i) {
            if (stripes[i].length() != 0) {
                String[] parts = stripes[i].split(",");
                result[i] = new RecordIdentifier();
                result[i].setValues(Long.parseLong(parts[0]), Integer.parseInt(parts[1]), Long.parseLong(parts[2]));
            }
        }

        return result;
    }

    static String parseBFColumns(Reader reader) {
        try {
            ByteBuffer val = reader.getMetadataValue("orc.bloomfilter.columns").duplicate();
            String columns = utf8.newDecoder().decode(val).toString();
            return columns;
        } catch (CharacterCodingException var3) {
            throw new IllegalArgumentException("Bad string encoding for hive.acid.key.index", var3);
        }
    }

    static String parseColumnIndexConcatStr(Reader reader) {
        ByteBuffer val;
        try {
            val = reader.getMetadataValue("orc.column.index.concat.str").duplicate();
        } catch (IllegalArgumentException var5) {
            return "all";
        }

        try {
            String ret = utf8.newDecoder().decode(val).toString();
            return ret;
        } catch (CharacterCodingException var4) {
            throw new IllegalArgumentException("Bad string encoding for hive.acid.key.index", var4);
        }
    }

    private static class RecIdStrippingObjectInspector extends StructObjectInspector {
        private StructObjectInspector wrapped;
        List<StructField> fields;
        StructField recId;

        RecIdStrippingObjectInspector(ObjectInspector oi, int rowIdColNum) {
            if (!(oi instanceof StructObjectInspector)) {
                throw new RuntimeException("Serious problem, expected a StructObjectInspector, but got a " + oi.getClass().getName());
            } else {
                this.wrapped = (StructObjectInspector)oi;
                List<? extends StructField> wrappedFields = this.wrapped.getAllStructFieldRefs();
                this.fields = new ArrayList(this.wrapped.getAllStructFieldRefs().size());

                for(int i = 0; i < wrappedFields.size(); ++i) {
                    if (i == rowIdColNum) {
                        this.recId = (StructField)wrappedFields.get(i);
                    } else {
                        this.fields.add(wrappedFields.get(i));
                    }
                }

            }
        }

        public List<? extends StructField> getAllStructFieldRefs() {
            return this.fields;
        }

        public StructField getStructFieldRef(String fieldName) {
            return this.wrapped.getStructFieldRef(fieldName);
        }

        public Object getStructFieldData(Object data, StructField fieldRef) {
            return this.wrapped.getStructFieldData(data, fieldRef);
        }

        public List<Object> getStructFieldsDataAsList(Object data) {
            return this.wrapped.getStructFieldsDataAsList(data);
        }

        public String getTypeName() {
            return this.wrapped.getTypeName();
        }

        public Category getCategory() {
            return this.wrapped.getCategory();
        }

        StructField getRecId() {
            return this.recId;
        }
    }

    static class KeyIndexBuilder implements WriterCallback {
        StringBuilder lastKey = new StringBuilder();
        long lastTransaction;
        int lastBucket;
        long lastRowId;
        AcidStats acidStats = new AcidStats();
        String bfColumns;
        String columnIndexConcatStr;

        KeyIndexBuilder() {
        }

        public void preStripeWrite(WriterContext context) throws IOException {
            this.lastKey.append(this.lastTransaction);
            this.lastKey.append(',');
            this.lastKey.append(this.lastBucket);
            this.lastKey.append(',');
            this.lastKey.append(this.lastRowId);
            this.lastKey.append(';');
        }

        public void preFooterWrite(WriterContext context) throws IOException {
            context.getWriter().addUserMetadata("hive.acid.key.index", OrcRecordUpdater.UTF8.encode(this.lastKey.toString()));
            context.getWriter().addUserMetadata("hive.acid.stats", OrcRecordUpdater.UTF8.encode(this.acidStats.serialize()));
            context.getWriter().addUserMetadata("orc.bloomfilter.columns", OrcRecordUpdater.UTF8.encode(this.bfColumns));
            context.getWriter().addUserMetadata("orc.column.index.concat.str", OrcRecordUpdater.UTF8.encode(this.columnIndexConcatStr));
        }

        public void setBFColumns(String bfColumns) {
            this.bfColumns = bfColumns;
        }

        public void setColumnIndexConcatStr(String columnIndexConcatStr) {
            this.columnIndexConcatStr = columnIndexConcatStr;
        }

        void addKey(int op, long transaction, int bucket, long rowId) {
            switch(op) {
                case 0:
                    ++this.acidStats.inserts;
                    break;
                case 1:
                    ++this.acidStats.updates;
                    break;
                case 2:
                    ++this.acidStats.deletes;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operation " + op);
            }

            this.lastTransaction = transaction;
            this.lastBucket = bucket;
            this.lastRowId = rowId;
        }
    }

    public static class OrcOptions extends Options {
        WriterOptions orcOptions = null;

        public OrcOptions(Configuration conf) {
            super(conf);
        }

        public OrcOptions orcOptions(WriterOptions opts) {
            this.orcOptions = opts;
            return this;
        }

        public WriterOptions getOrcOptions() {
            return this.orcOptions;
        }
    }

    static class AcidStats {
        long inserts;
        long updates;
        long deletes;

        AcidStats() {
        }

        AcidStats(String serialized) {
            String[] parts = serialized.split(",");
            this.inserts = Long.parseLong(parts[0]);
            this.updates = Long.parseLong(parts[1]);
            this.deletes = Long.parseLong(parts[2]);
        }

        String serialize() {
            StringBuilder builder = new StringBuilder();
            builder.append(this.inserts);
            builder.append(",");
            builder.append(this.updates);
            builder.append(",");
            builder.append(this.deletes);
            return builder.toString();
        }
    }
}
