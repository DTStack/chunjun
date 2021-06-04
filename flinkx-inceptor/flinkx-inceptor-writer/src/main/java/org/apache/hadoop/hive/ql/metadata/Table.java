package org.apache.hadoop.hive.ql.metadata;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.tableattributes.StorageDDLSupportedOperation;
import org.apache.hadoop.hive.common.tableattributes.StorageType;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.ProtectMode;
import org.apache.hadoop.hive.metastore.TableSerdeType;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.DbLink;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionRange;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.io.StellarDBInputFormat;
import org.apache.hadoop.hive.ql.metadata.tableattributes.InceptorTableAttributes;
import org.apache.hadoop.hive.ql.metadata.tableattributes.TableAttributesFactory;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ErrorTable.ErrorTableSetting;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.SortedPartSpec;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.util.ErrorMsgUtil;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

public class Table implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Log LOG = LogFactory.getLog("hive.ql.metadata.Table");

    private org.apache.hadoop.hive.metastore.api.Table tTable;

    private DbLink dbLink;

    private transient Deserializer deserializer;

    private Class<? extends OutputFormat> outputFormatClass;

    private Class<? extends InputFormat> inputFormatClass;

    private Path path;

    private HiveStorageHandler storageHandler;

    private PrimaryKeyInfo pkInfo;

    private transient BaseSemanticAnalyzer.tableSpec tSpec;

    private transient InceptorTableAttributes inceptorTableAttributes;

    public Table() {}

    public Table(org.apache.hadoop.hive.metastore.api.Table table) {
        initialize(table);
    }

    protected void initialize(org.apache.hadoop.hive.metastore.api.Table table) {
        this.tTable = table;
        if (!isView()) {
            getInputFormatClass();
            getOutputFormatClass();
        }
    }

    public Table(String databaseName, String tableName) {
        this(getEmptyTable(databaseName, tableName));
    }

    public boolean isDummyTable() {
        return this.tTable.getTableName().equals("_dummy_table");
    }

    public org.apache.hadoop.hive.metastore.api.Table getTTable() {
        return this.tTable;
    }

    public void setTTable(org.apache.hadoop.hive.metastore.api.Table tTable) {
        this.tTable = tTable;
    }

    static org.apache.hadoop.hive.metastore.api.Table getEmptyTable(String databaseName, String tableName) {
        StorageDescriptor sd = new StorageDescriptor();
        sd.setSerdeInfo(new SerDeInfo());
        sd.setNumBuckets(-1);
        sd.setBucketCols(new ArrayList());
        sd.setCols(new ArrayList());
        sd.setParameters(new HashMap<>());
        sd.setSortCols(new ArrayList());
        sd.getSerdeInfo().setParameters(new HashMap<>());
        sd.getSerdeInfo().setSerializationLib(MetadataTypedColumnsetSerDe.class.getName());
        sd.getSerdeInfo().getParameters().put("serialization.format", "1");
        sd.setInputFormat(SequenceFileInputFormat.class.getName());
        sd.setOutputFormat(HiveSequenceFileOutputFormat.class.getName());
        SkewedInfo skewInfo = new SkewedInfo();
        skewInfo.setSkewedColNames(new ArrayList());
        skewInfo.setSkewedColValues(new ArrayList());
        skewInfo.setSkewedColValueLocationMaps(new HashMap<>());
        sd.setSkewedInfo(skewInfo);
        org.apache.hadoop.hive.metastore.api.Table t = new org.apache.hadoop.hive.metastore.api.Table();
        t.setSd(sd);
        t.setPartitionKeys(new ArrayList());
        t.setParameters(new HashMap<>());
        t.setTableType(TableType.MANAGED_TABLE.toString());
        t.setDbName(databaseName);
        t.setTableName(tableName);
        t.setOwner(SessionState.getUserFromAuthenticator());
        t.setCreateTime((int)(System.currentTimeMillis() / 1000L));
        return t;
    }

    public void checkValidity() throws HiveException {
        String name = this.tTable.getTableName();
        if (null == name || name.length() == 0 || (getDbLink() == null && !MetaStoreUtils.validateName(name)) || (getDbLink() != null && !MetaStoreUtils.validateDbLinkTableName(name)) || !MetaStoreUtils.validateNameInPath(name))
            throw new HiveException(ErrorMsg.ERROR_20479, new String[] { ErrorMsgUtil.toString(name) });
        boolean tableAllowNoColumn = false;
        if (this.tTable.getSd().getInputFormat().equals(StellarDBInputFormat.class.getName()))
            tableAllowNoColumn = true;
        if (!tableAllowNoColumn && 0 == getCols().size())
            throw new HiveException(ErrorMsg.ERROR_20480, new String[0]);
        if (!isView()) {
            if (null == getDeserializerFromMetaStore(false))
                throw new HiveException(ErrorMsg.ERROR_20481, new String[0]);
            if (null == getInputFormatClass())
                throw new HiveException(ErrorMsg.ERROR_20482, new String[0]);
            if (null == getOutputFormatClass())
                throw new HiveException(ErrorMsg.ERROR_20483, new String[0]);
        }
        if (isView()) {
            assert getViewOriginalText() != null;
            assert getViewExpandedText() != null;
        } else if (isMaterializedView()) {
            assert getViewOriginalText() != null;
            assert getViewExpandedText() != null;
        } else {
            assert getViewOriginalText() == null;
            assert getViewExpandedText() == null;
        }
        Iterator<FieldSchema> iterCols = getCols().iterator();
        List<String> colNames = new ArrayList<>();
        while (iterCols.hasNext()) {
            String colName = ((FieldSchema)iterCols.next()).getName();
            if (!MetaStoreUtils.validateName(colName))
                throw new HiveException(ErrorMsg.ERROR_20484, new String[] { ErrorMsgUtil.toString(colName) });
            Iterator<String> iter = colNames.iterator();
            while (iter.hasNext()) {
                String oldColName = iter.next();
                if (colName.equalsIgnoreCase(oldColName))
                    throw new HiveException(ErrorMsg.ERROR_20485, new String[] { ErrorMsgUtil.toString(colName) });
            }
            colNames.add(colName.toLowerCase());
        }
        if (isPartitioned() && !isPartitionSupported())
            throw new SemanticException(ErrorMsg.PARTITION_NOT_SUPPORTED, new String[] { getStorageName() });
        if (getPartCols() != null && !isRangePartitioned()) {
            Iterator<FieldSchema> partColsIter = getPartCols().iterator();
            while (partColsIter.hasNext()) {
                String partCol = ((FieldSchema)partColsIter.next()).getName();
                if (!MetaStoreUtils.validateNameInPath(partCol))
                    throw new HiveException(ErrorMsg.INVALID_PARTITION_COLUMN_NAME, new String[] { ErrorMsgUtil.toString(partCol) });
                if (colNames.contains(partCol.toLowerCase()))
                    throw new HiveException(ErrorMsg.ERROR_20486, new String[] { ErrorMsgUtil.toString(partCol) });
            }
        }
    }

    public StorageDescriptor getSd() {
        return this.tTable.getSd();
    }

    public void setInputFormatClass(Class<? extends InputFormat> inputFormatClass) {
        this.inputFormatClass = inputFormatClass;
        this.tTable.getSd().setInputFormat(inputFormatClass.getName());
    }

    public void setOutputFormatClass(Class<? extends OutputFormat> outputFormatClass) {
        this.outputFormatClass = outputFormatClass;
        this.tTable.getSd().setOutputFormat(outputFormatClass.getName());
    }

    public final Properties getMetadata() {
        return MetaStoreUtils.getTableMetadata(this.tTable);
    }

    public final Path getPath() {
        String location = this.tTable.getSd().getLocation();
        if (location == null)
            return null;
        return new Path(location);
    }

    public final String getTableName() {
        return this.tTable.getTableName();
    }

    public final Path getDataLocation() {
        if (this.path == null)
            this.path = getPath();
        return this.path;
    }

    public final Deserializer getDeserializer() {
        if (this.deserializer == null)
            this.deserializer = getDeserializerFromMetaStore(false);
        return this.deserializer;
    }

    public final Class<? extends Deserializer> getDeserializerClass() throws Exception {
        return MetaStoreUtils.getDeserializerClass((Configuration)Hive.get().getConf(), this.tTable);
    }

    public final Deserializer getDeserializer(boolean skipConfError) {
        if (this.deserializer == null)
            this.deserializer = getDeserializerFromMetaStore(skipConfError);
        return this.deserializer;
    }

    public final Deserializer getDeserializerFromMetaStore(boolean skipConfError) {
        try {
            return MetaStoreUtils.getDeserializer((Configuration)Hive.get().getConf(), this.tTable, skipConfError);
        } catch (MetaException e) {
            throw new RuntimeException(e);
        } catch (HiveException e) {
            throw new RuntimeException(e);
        }
    }

    public HiveStorageHandler getStorageHandler() {
        if (this.storageHandler != null || !isNonNative())
            return this.storageHandler;
        try {
            this.storageHandler = HiveUtils.getStorageHandler((Configuration)Hive.get().getConf(), getProperty("storage_handler"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return this.storageHandler;
    }

    public final Class<? extends InputFormat> getInputFormatClass() {
        if (this.inputFormatClass == null)
            try {
//                String className = this.tTable.getSd().getInputFormat();
                String className = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
                if (className == null) {
                    if (getStorageHandler() == null)
                        return null;
                    this.inputFormatClass = getStorageHandler().getInputFormatClass();
                } else {
                    if (className.equalsIgnoreCase("shark.memstore2.MemoryTableInputFormat"))
                        className = "io.transwarp.inceptor.memstore2.MemoryTableInputFormat";
                    this.inputFormatClass = (Class)Class.forName(className, true, JavaUtils.getClassLoader());
                }
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        return this.inputFormatClass;
    }

    public final Class<? extends OutputFormat> getOutputFormatClass() {
        boolean storagehandler = false;
        if (this.outputFormatClass == null)
            try {
                Class<?> c;
                //String className = this.tTable.getSd().getOutputFormat();
                String className = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
                if (className == null) {
                    if (getStorageHandler() == null)
                        return null;
                    c = getStorageHandler().getOutputFormatClass();
                } else {
                    c = Class.forName(className, true, JavaUtils.getClassLoader());
                }
                if (!HiveOutputFormat.class.isAssignableFrom(c)) {
                    if (getStorageHandler() != null) {
                        storagehandler = true;
                    } else {
                        storagehandler = false;
                    }
                    this.outputFormatClass = HiveFileFormatUtils.getOutputFormatSubstitute(c);
                } else {
                    this.outputFormatClass = (Class)c;
                }
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        return this.outputFormatClass;
    }

    public final void validatePartColumnNames(Map<String, String> spec, boolean shouldBeFull) throws SemanticException {
        List<FieldSchema> partCols = this.tTable.getPartitionKeys();
        if (partCols == null || partCols.size() == 0) {
            if (spec != null)
                throw new SemanticException(ErrorMsg.ERROR_11274, new String[] { ErrorMsgUtil.toString(spec) });
            return;
        }
        if (spec == null) {
            if (shouldBeFull)
                throw new SemanticException(ErrorMsg.ERROR_11275, new String[0]);
            return;
        }
        int columnsFound = 0;
        for (FieldSchema fs : partCols) {
            if (spec.containsKey(fs.getName()))
                columnsFound++;
            if (columnsFound == spec.size())
                break;
        }
        if (columnsFound < spec.size())
            throw new SemanticException(ErrorMsg.ERROR_11276, new String[] { ErrorMsgUtil.toString(spec) });
        if (shouldBeFull && spec.size() != partCols.size())
            throw new SemanticException(ErrorMsg.ERROR_11277, new String[] { ErrorMsgUtil.toString(spec), ErrorMsgUtil.toString(Integer.valueOf(partCols.size())) });
    }

    public final boolean isValidSpec(Map<String, String> spec) throws HiveException {
        List<FieldSchema> partCols = this.tTable.getPartitionKeys();
        if (partCols == null || partCols.size() == 0) {
            if (spec != null)
                throw new HiveException(ErrorMsg.ERROR_11274, new String[] { ErrorMsgUtil.toString(spec) });
            return true;
        }
        if (spec == null || spec.size() != partCols.size())
            throw new HiveException(ErrorMsg.ERROR_20487, new String[] { ErrorMsgUtil.toString(spec) });
        for (FieldSchema field : partCols) {
            if (!spec.containsKey(field.getName()))
                throw new HiveException(ErrorMsg.PARTITION_OMITED, new String[] { field.getName(), spec.toString() });
            if (spec.get(field.getName()) == null)
                throw new HiveException(ErrorMsg.PARTITION_NO_SPEC, new String[] { field.getName(), spec.toString() });
        }
        return true;
    }

    public void setProperty(String name, String value) {
        this.tTable.getParameters().put(name, value);
    }

    public void setParamters(Map<String, String> params) {
        this.tTable.setParameters(params);
    }

    public String getProperty(String name) {
        return (String)this.tTable.getParameters().get(name);
    }

    public boolean isImmutable() {
        return (this.tTable.getParameters().containsKey("immutable") && ((String)this.tTable.getParameters().get("immutable")).equalsIgnoreCase("true"));
    }

    public boolean isStreaming() {
        return (this.tTable.getParameters().containsKey("STREAMING") && ((String)this.tTable.getParameters().get("STREAMING")).equalsIgnoreCase("true"));
    }

    public boolean isStreamMetric() {
        return (this.tTable.getParameters().containsKey("StreamMetric") && ((String)this.tTable.getParameters().get("StreamMetric")).equalsIgnoreCase("true"));
    }

    public void setTableType(TableType tableType) {
        this.tTable.setTableType(tableType.toString());
    }

    public TableType getTableType() {
        return Enum.<TableType>valueOf(TableType.class, this.tTable.getTableType());
    }

    public void setErrorTableSetting(ErrorTableSetting setting) {
        if (setting != null) {
            this.tTable.setErrorLogEnable(setting.isErrorLogEnable());
            this.tTable.setErrorTableName(setting.getErrorTableName());
            this.tTable.setOverwrite(setting.isOverwriteOn());
            this.tTable.setRejectNumRows(setting.getRowCount());
            this.tTable.setRejectRowsPercent(setting.getRowPercent());
        } else if (this.tTable.getErrorTableName() != null) {
            this.tTable.setErrorLogEnable(false);
        } else {
            this.tTable.setErrorTableName(null);
            this.tTable.setRejectNumRows(-1);
            this.tTable.setOverwrite(false);
            this.tTable.setErrorLogEnable(false);
        }
    }

    public ArrayList<StructField> getFields() {
        ArrayList<StructField> fields = new ArrayList<>();
        try {
            Deserializer decoder = getDeserializer();
            StructObjectInspector structObjectInspector = (StructObjectInspector)decoder.getObjectInspector();
            List<? extends StructField> fld_lst = structObjectInspector.getAllStructFieldRefs();
            for (StructField field : fld_lst)
                fields.add(field);
        } catch (SerDeException e) {
            throw new RuntimeException(e);
        }
        return fields;
    }

    public StructField getField(String fld) {
        try {
            StructObjectInspector structObjectInspector = (StructObjectInspector)getDeserializer().getObjectInspector();
            return structObjectInspector.getStructFieldRef(fld);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String toString() {
        return this.tTable.getTableName();
    }

    public int hashCode() {
        int prime = 31;
        int result = 1;
        result = 31 * result + ((this.tTable == null) ? 0 : this.tTable.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Table other = (Table)obj;
        if (this.tTable == null) {
            if (other.tTable != null)
                return false;
        } else if (!this.tTable.equals(other.tTable)) {
            return false;
        }
        return true;
    }

    public List<FieldSchema> getPartCols() {
        List<FieldSchema> partKeys = this.tTable.getPartitionKeys();
        if (partKeys == null) {
            partKeys = new ArrayList<>();
            this.tTable.setPartitionKeys(partKeys);
        }
        return partKeys;
    }

    public boolean isPartitionKey(String colName) {
        for (FieldSchema key : getPartCols()) {
            if (key.getName().toLowerCase().equals(colName))
                return true;
        }
        return false;
    }

    public String getBucketingDimensionId() {
        List<String> bcols = this.tTable.getSd().getBucketCols();
        if (bcols == null || bcols.size() == 0)
            return null;
        if (bcols.size() > 1)
            LOG.warn(this + " table has more than one dimensions which aren't supported yet");
        return bcols.get(0);
    }

    public void setDataLocation(Path path) {
        this.path = path;
        this.tTable.getSd().setLocation(path.toString());
    }

    public void unsetDataLocation() {
        this.path = null;
        this.tTable.getSd().unsetLocation();
    }

    public void setBucketCols(List<String> bucketCols) throws HiveException {
        if (bucketCols == null)
            return;
        for (String col : bucketCols) {
            if (!isField(col))
                throw new HiveException(ErrorMsg.ERROR_20489, new String[] { ErrorMsgUtil.toString(col), ErrorMsgUtil.toString(getCols()) });
        }
        this.tTable.getSd().setBucketCols(bucketCols);
    }

    public void setSortCols(List<Order> sortOrder) throws HiveException {
        this.tTable.getSd().setSortCols(sortOrder);
    }

    public void setSkewedValueLocationMap(List<String> valList, String dirName) throws HiveException {
        Map<List<String>, String> mappings = this.tTable.getSd().getSkewedInfo().getSkewedColValueLocationMaps();
        if (null == mappings) {
            mappings = new HashMap<>();
            this.tTable.getSd().getSkewedInfo().setSkewedColValueLocationMaps(mappings);
        }
        mappings.put(valList, dirName);
    }

    public Map<List<String>, String> getSkewedColValueLocationMaps() {
        return (this.tTable.getSd().getSkewedInfo() != null) ? this.tTable.getSd().getSkewedInfo().getSkewedColValueLocationMaps() : new HashMap<>();
    }

    public void setSkewedColValues(List<List<String>> skewedValues) throws HiveException {
        this.tTable.getSd().getSkewedInfo().setSkewedColValues(skewedValues);
    }

    public List<List<String>> getSkewedColValues() {
        return (this.tTable.getSd().getSkewedInfo() != null) ? this.tTable.getSd().getSkewedInfo().getSkewedColValues() : new ArrayList<>();
    }

    public void setSkewedColNames(List<String> skewedColNames) throws HiveException {
        this.tTable.getSd().getSkewedInfo().setSkewedColNames(skewedColNames);
    }

    public List<String> getSkewedColNames() {
        return (this.tTable.getSd().getSkewedInfo() != null) ? this.tTable.getSd().getSkewedInfo().getSkewedColNames() : new ArrayList<>();
    }

    public SkewedInfo getSkewedInfo() {
        return this.tTable.getSd().getSkewedInfo();
    }

    public void setSkewedInfo(SkewedInfo skewedInfo) throws HiveException {
        this.tTable.getSd().setSkewedInfo(skewedInfo);
    }

    public boolean isStoredAsSubDirectories() {
        return this.tTable.getSd().isStoredAsSubDirectories();
    }

    public void setStoredAsSubDirectories(boolean storedAsSubDirectories) throws HiveException {
        this.tTable.getSd().setStoredAsSubDirectories(storedAsSubDirectories);
    }

    private boolean isField(String col) {
        for (FieldSchema field : getCols()) {
            if (field.getName().equals(col))
                return true;
        }
        return false;
    }

    public List<FieldSchema> getCols() {
        boolean getColsFromSerDe = false;
        try {
            getColsFromSerDe = !SerDeUtils.hasMetastoreBasedSchema(Hive.get().getConf(), getSerializationLib());
        } catch (HiveException e) {
            LOG.error("Unable to get hiveDB, " + e.getMessage(), e);
        }
        if (!getColsFromSerDe)
            return this.tTable.getSd().getCols();
        try {
            return Hive.getFieldsFromDeserializer(getTableName(), getDeserializer());
        } catch (HiveException e) {
            LOG.error("Unable to get field from serde: " + getSerializationLib(), e);
            return new ArrayList<>();
        }
    }

    public List<FieldSchema> getAllCols() {
        ArrayList<FieldSchema> f_list = new ArrayList<>();
        f_list.addAll(getCols());
        if (!this.tTable.isIsRangePartitioned())
            f_list.addAll(getPartCols());
        return f_list;
    }

    public void setPartCols(List<FieldSchema> partCols) {
        this.tTable.setPartitionKeys(partCols);
    }

    public String getDbName() {
        return this.tTable.getDbName();
    }

    public String getQualifiedName() {
        return (this.tTable.getDbName() + "." + this.tTable.getTableName()).toLowerCase();
    }

    public int getNumBuckets() {
        return this.tTable.getSd().getNumBuckets();
    }

    protected void replaceFiles(Path srcf, boolean isSrcLocal) throws HiveException {
        Path tableDest = getPath();
        Hive.replaceFiles(tableDest, srcf, tableDest, tableDest, Hive.get().getConf(), isSrcLocal);
    }

    protected void copyFiles(Path srcf, boolean isSrcLocal, boolean isAcid) throws HiveException {
        try {
            FileSystem fs = getDataLocation().getFileSystem((Configuration)Hive.get().getConf());
            Hive.copyFiles(Hive.get().getConf(), srcf, getPath(), fs, isSrcLocal, isAcid);
        } catch (IOException e) {
            throw new HiveException(e, ErrorMsg.ERROR_20218, new String[0]);
        }
    }

    public void setInputFormatClass(String name) throws HiveException {
        if (name == null) {
            this.inputFormatClass = null;
            this.tTable.getSd().setInputFormat(null);
            return;
        }
        try {
            setInputFormatClass((Class)Class.forName(name, true, Utilities.getSessionSpecifiedClassLoader()));
        } catch (ClassNotFoundException e) {
            throw new HiveException(e, ErrorMsg.ERROR_20476, new String[] { ErrorMsgUtil.toString(name) });
        }
    }

    public void setOutputFormatClass(String name) throws HiveException {
        if (name == null) {
            this.outputFormatClass = null;
            this.tTable.getSd().setOutputFormat(null);
            return;
        }
        try {
            Class<?> origin = Class.forName(name, true, Utilities.getSessionSpecifiedClassLoader());
            setOutputFormatClass(HiveFileFormatUtils.getOutputFormatSubstitute(origin));
        } catch (ClassNotFoundException e) {
            throw new HiveException(e, ErrorMsg.ERROR_20476, new String[] { ErrorMsgUtil.toString(name) });
        }
    }

    public boolean isPartitioned() {
        if (getPartCols() == null)
            return false;
        return (getPartCols().size() != 0);
    }

    public void setFields(List<FieldSchema> fields) {
        this.tTable.getSd().setCols(fields);
    }

    public void setNumBuckets(int nb) {
        this.tTable.getSd().setNumBuckets(nb);
    }

    public String getOwner() {
        return this.tTable.getOwner();
    }

    public Map<String, String> getParameters() {
        return this.tTable.getParameters();
    }

    public int getRetention() {
        return this.tTable.getRetention();
    }

    public void setOwner(String owner) {
        this.tTable.setOwner(owner);
    }

    public void setRetention(int retention) {
        this.tTable.setRetention(retention);
    }

    private SerDeInfo getSerdeInfo() {
        return this.tTable.getSd().getSerdeInfo();
    }

    public void setSerializationLib(String lib) {
        getSerdeInfo().setSerializationLib(lib);
    }

    public String getSerializationLib() {
        return getSerdeInfo().getSerializationLib();
    }

    public String getSerdeParam(String param) {
        return (String)getSerdeInfo().getParameters().get(param);
    }

    public String setSerdeParam(String param, String value) {
        return getSerdeInfo().getParameters().put(param, value);
    }

    public List<String> getBucketCols() {
        return this.tTable.getSd().getBucketCols();
    }

    public List<Order> getSortCols() {
        return this.tTable.getSd().getSortCols();
    }

    public void setTableName(String tableName) {
        this.tTable.setTableName(tableName);
    }

    public void setDbName(String databaseName) {
        this.tTable.setDbName(databaseName);
    }

    public List<FieldSchema> getPartitionKeys() {
        return this.tTable.getPartitionKeys();
    }

    public String getViewOriginalText() {
        return this.tTable.getViewOriginalText();
    }

    public void setViewOriginalText(String viewOriginalText) {
        this.tTable.setViewOriginalText(viewOriginalText);
    }

    public String getViewExpandedText() {
        return this.tTable.getViewExpandedText();
    }

    public void clearSerDeInfo() {
        this.tTable.getSd().getSerdeInfo().getParameters().clear();
    }

    public void setViewExpandedText(String viewExpandedText) {
        this.tTable.setViewExpandedText(viewExpandedText);
    }

    public boolean isView() {
        return TableType.VIRTUAL_VIEW.equals(getTableType());
    }

    public boolean isMaterializedView() {
        return TableType.MATERIALIZED_VIEW.equals(getTableType());
    }

    public boolean isIndexTable() {
        return TableType.INDEX_TABLE.equals(getTableType());
    }

    public boolean isRlsEnabled() {
        return this.tTable.isRlsEnabled();
    }

    public void setRlsEnabled(boolean rlsEnabled) {
        this.tTable.setRlsEnabled(rlsEnabled);
    }

    public String getRowPermission() {
        return this.tTable.getRowPermission();
    }

    public void setRowPermission(String rowPermission) {
        this.tTable.setRowPermission(rowPermission);
    }

    public boolean isClsEnabled() {
        return this.tTable.isClsEnabled();
    }

    public void setClsEnabled(boolean clsEnabled) {
        this.tTable.setClsEnabled(clsEnabled);
    }

    public String getColumnPermission() {
        return this.tTable.getColumnPermission();
    }

    public void setColumnPermission(String columnPermission) {
        this.tTable.setColumnPermission(columnPermission);
    }

    public boolean isRewriteEnabled() {
        return this.tTable.isRewriteEnabled();
    }

    public void setRewriteEnabled(boolean rewriteEnabled) {
        this.tTable.setRewriteEnabled(rewriteEnabled);
    }

    public int getCreateTime() {
        return this.tTable.getCreateTime();
    }

    public SortedPartSpec createSpec(Partition tp) {
        SortedPartSpec ret = new SortedPartSpec();
        if (tp.isSetRanges() && tp.getRanges().size() > 0) {
            ret.setRangePart(true);
            List<String> partName = new ArrayList<>();
            partName.add(tp.getPartName());
            ret.setRangePartSpec(partName);
        } else {
            List<FieldSchema> fsl = getPartCols();
            List<String> tpl = tp.getValues();
            LinkedHashMap<String, String> spec = new LinkedHashMap<>();
            for (int i = 0; i < fsl.size(); i++) {
                FieldSchema fs = fsl.get(i);
                String value = tpl.get(i);
                spec.put(fs.getName(), value);
            }
            ret.setRangePart(false);
            ret.setUniquePartSpec(spec);
        }
        return ret;
    }

    public Table copy() throws HiveException {
        return new Table(this.tTable.deepCopy());
    }

    public void setCreateTime(int createTime) {
        this.tTable.setCreateTime(createTime);
    }

    public int getLastAccessTime() {
        return this.tTable.getLastAccessTime();
    }

    public void setLastAccessTime(int lastAccessTime) {
        this.tTable.setLastAccessTime(lastAccessTime);
    }

    public boolean isNonNative() {
        return (getProperty("storage_handler") != null);
    }

    public void setProtectMode(ProtectMode protectMode) {
        Map<String, String> parameters = this.tTable.getParameters();
        String pm = protectMode.toString();
        if (pm != null) {
            parameters.put("PROTECT_MODE", pm);
        } else {
            parameters.remove("PROTECT_MODE");
        }
        this.tTable.setParameters(parameters);
    }

    public ProtectMode getProtectMode() {
        Map<String, String> parameters = this.tTable.getParameters();
        if (!parameters.containsKey("PROTECT_MODE"))
            return new ProtectMode();
        return ProtectMode.getProtectModeFromString(parameters.get("PROTECT_MODE"));
    }

    public boolean isOffline() {
        return (getProtectMode()).offline;
    }

    public boolean canDrop() {
        ProtectMode mode = getProtectMode();
        return (!mode.noDrop && !mode.offline && !mode.readOnly && !mode.noDropCascade);
    }

    public boolean canWrite() {
        ProtectMode mode = getProtectMode();
        return (!mode.offline && !mode.readOnly);
    }

    public String getCompleteName() {
        return getDbName() + "@" + getTableName();
    }

    public static String getCompleteName(String dbName, String tabName) {
        return dbName + "@" + tabName;
    }

    public List<Index> getAllIndexes(short max) throws HiveException {
        Hive hive = Hive.get();
        return hive.getIndexes(getTTable().getDbName(), getTTable().getTableName(), max);
    }

    public FileStatus[] getSortedPaths() {
        try {
            FileSystem fs = FileSystem.get(getPath().toUri(), (Configuration)Hive.get().getConf());
            String pathPattern = getPath().toString();
            if (getNumBuckets() > 0)
                pathPattern = pathPattern + "/*";
            LOG.info("Path pattern = " + pathPattern);
            FileStatus[] srcs = fs.globStatus(new Path(pathPattern));
            Arrays.sort((Object[])srcs);
            for (FileStatus src : srcs)
                LOG.info("Got file: " + src.getPath());
            if (srcs.length == 0)
                return null;
            return srcs;
        } catch (Exception e) {
            throw new RuntimeException("Cannot get path ", e);
        }
    }

    public boolean isTemporary() {
        return this.tTable.isTemporary();
    }

    public boolean isValidRangeSpec(List<PartitionRange> ranges) throws HiveException {
        List<FieldSchema> partCols = this.tTable.getPartitionKeys();
        if (partCols == null || partCols.size() == 0) {
            if (ranges != null)
                throw new HiveException(ErrorMsg.ERROR_20490, new String[] { ErrorMsgUtil.toString(ranges) });
            return true;
        }
        if (ranges == null || ranges.size() != partCols.size())
            throw new HiveException(ErrorMsg.ERROR_20487, new String[] { ErrorMsgUtil.toString(ranges) });
        return true;
    }

    public boolean isRangePartitioned() {
        return this.tTable.isIsRangePartitioned();
    }

    public boolean fieldHasNotNullConstraint(int index) {
        if (this.tTable.getSd().getCols().size() <= index)
            return false;
        return ((FieldSchema)this.tTable.getSd().getCols().get(index)).isNotNullConstraint();
    }

    public boolean fieldHasUniqueConstraint(int index) {
        if (this.tTable.getSd().getCols().size() <= index)
            return false;
        return ((FieldSchema)this.tTable.getSd().getCols().get(index)).isSetUniqueConstraint();
    }

    public void setIsRangePartitioned(boolean b) {
        this.tTable.setIsRangePartitioned(b);
    }

    public DbLink getDbLink() {
        return this.dbLink;
    }

    public ErrorTableSetting getErrorTableSetting() {
        if (this.tTable.getErrorTableName() != null) {
            ErrorTableSetting setting = null;
            if (this.tTable.isErrorLogEnable()) {
                if (this.tTable.getRejectNumRows() >= 0) {
                    setting = new ErrorTableSetting(this.tTable.getErrorTableName(), this.tTable.getRejectNumRows(), this.tTable.isOverwrite());
                } else if (this.tTable.getRejectRowsPercent() > 0.0D) {
                    setting = new ErrorTableSetting(this.tTable.getErrorTableName(), this.tTable.getRejectRowsPercent(), this.tTable.isOverwrite());
                } else {
                    setting = new ErrorTableSetting(this.tTable.getErrorTableName(), this.tTable.isOverwrite());
                }
            } else {
                setting = new ErrorTableSetting(this.tTable.getErrorTableName());
                setting.setErrorLogEnable(false);
            }
            return setting;
        }
        return null;
    }

    public void setErrorTableName(String errorTableName) {
        this.tTable.setErrorTableName(errorTableName);
    }

    public void setRejectRowsNumber(int rejectRowsNumber) {
        this.tTable.setRejectNumRows(rejectRowsNumber);
    }

    public void setRejectRowsPercent(double rejectRowsPercent) {
        this.tTable.setRejectRowsPercent(rejectRowsPercent);
    }

    public void setOverwrittenOn(boolean overwrittenOn) {
        this.tTable.setOverwrite(overwrittenOn);
    }

    public void setErrorLoggingOn(boolean errorLoggingOn) {
        this.tTable.setErrorLogEnable(errorLoggingOn);
    }

    public void setDbLink(DbLink dbLink) {
        this.dbLink = dbLink;
    }

    public BaseSemanticAnalyzer.tableSpec getTableSpec() {
        return this.tSpec;
    }

    public void setTableSpec(BaseSemanticAnalyzer.tableSpec tableSpec1) {
        this.tSpec = tableSpec1;
    }

    public InceptorTableAttributes getInceptorTableAttributes() {
        if (this.inceptorTableAttributes != null)
            return this.inceptorTableAttributes;
        synchronized (this) {
            try {
                this.inceptorTableAttributes = TableAttributesFactory.getTableAttributes(this, (Configuration)Hive.get().getConf());
            } catch (HiveException e) {
                LOG.error("Get hive conf failed when analyze the attributes from table, set the HiveConf as null");
                throw new RuntimeException(e);
            }
        }
        return this.inceptorTableAttributes;
    }

    public void setMVSourceTables(List<String> mvSourceTables) {
        this.tTable.setMvSourceTables(mvSourceTables);
    }

    public List<String> getMVSourceTables() {
        return this.tTable.getMvSourceTables();
    }

    private static String normalize(String colName) throws HiveException {
        if (!MetaStoreUtils.validateColumnName(colName))
            throw new HiveException(ErrorMsg.ERROR_20484, new String[] { ErrorMsgUtil.toString(colName) });
        return colName.toLowerCase();
    }

    public boolean isOrcTable() {
        TableSerdeType t = getTableSerdeType();
        return (t == TableSerdeType.ORC_TRANSACTION || t == TableSerdeType.ORC);
    }

    public TableSerdeType getTableSerdeType() {
        String serde = getSd().getSerdeInfo().getSerializationLib();
        if (serde == null || getInputFormatClass() == null)
            return isView() ? TableSerdeType.VIEW : TableSerdeType.NONE;
        if (serde.equals("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
            return TableSerdeType.TEXT;
        if (serde.equals("org.apache.hadoop.hive.ql.io.csv.serde.CSVSerde"))
            return TableSerdeType.CSV;
        if (serde.equals("org.apache.hadoop.hive.ql.io.orc.OrcSerde")) {
            Properties props = getMetadata();
            return (props.containsKey("transactional") && props.get("transactional").equals("true")) ? TableSerdeType.ORC_TRANSACTION : TableSerdeType.ORC;
        }
        if (getInputFormatClass().getName().equals("org.apache.hadoop.hive.hbase.transaction.HBaseTransactionInputFormat"))
            return TableSerdeType.HBASE_TRANSACTION;
        if (serde.equals("org.apache.hadoop.hive.hbase.HBaseSerDe"))
            return TableSerdeType.HBASE;
        if (serde.equals("io.transwarp.inceptor.memstore2.LazySimpleSerDeWrapper"))
            return TableSerdeType.HOLODESK;
        if (serde.equals("io.transwarp.esdrive.serde.ElasticSearchSerDe"))
            return TableSerdeType.ES;
        if (serde.equals("org.apache.hadoop.hive.ql.io.fwc.serde.FWCSerde"))
            return TableSerdeType.FWC;
        if (serde.equals("org.apache.hadoop.hive.serde2.avro.AvroSerDe"))
            return TableSerdeType.AVRO;
        if (serde.equals("io.transwarp.hyperbase.HyperbaseSerDe"))
            return TableSerdeType.HYPERBASE;
        if (serde.equals("io.transwarp.hyperdrive.serde.HyperdriveSerDe"))
            return TableSerdeType.HYPERDRIVE;
        if (serde.equals("org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe"))
            return TableSerdeType.MUTIDELIMITED;
        return TableSerdeType.OTHERS;
    }

    public long getLastModifiedTime() {
        long ddlTime = -1L;
        long loadTime = -1L;
        if (this.tTable.getParameters().get("transient_lastDdlTime") != null)
            ddlTime = Long.parseLong((String)this.tTable.getParameters().get("transient_lastDdlTime"));
        if (this.tTable.getParameters().get("last_load_time") != null)
            loadTime = Long.parseLong((String)this.tTable.getParameters().get("last_load_time"));
        return Math.max(loadTime, ddlTime);
    }

    public long getLastRefreshTime() {
        if (this.tTable.getParameters().get("last_refresh_time") != null)
            return Long.parseLong((String)this.tTable.getParameters().get("last_refresh_time"));
        return -1L;
    }

    public boolean isStargateTable() {
        return getInceptorTableAttributes().isStargateTable();
    }

    public boolean isInsertPartialColumnSupported() {
        return getInceptorTableAttributes().isInsertPartialColumnSupported();
    }

    public boolean isCrudPartialColumnSupported() {
        return getInceptorTableAttributes().isCrudPartialColumnSupported();
    }

    public List<Integer> getPrimaryKeyOffsetList() throws HiveException {
        return getInceptorTableAttributes().getPrimaryKeyOffsetList();
    }

    public boolean isInsertPrimaryKeyNeeded() {
        return getInceptorTableAttributes().isInsertPrimaryKeyNeeded();
    }

    public boolean isInsertExternalColBesidesPrimaryKeyNeeded() {
        return getInceptorTableAttributes().isInsertExternalColBesidesPrimaryKeyNeeded();
    }

    public boolean isInsertValuesSupported() {
        return getInceptorTableAttributes().isInsertValuesSupported();
    }

    public boolean isPartitionSupported() {
        return getInceptorTableAttributes().isPartitionSupported();
    }

    public boolean isDynamicPartitionSupported() {
        return getInceptorTableAttributes().isDynamicPartitionSupported();
    }

    public boolean isInsertOverwriteSupported() {
        return getInceptorTableAttributes().isInsertOverwriteSupported();
    }

    public boolean isDeletePropertyNeeded() {
        return getInceptorTableAttributes().isDeletePropertyNeeded();
    }

    public boolean isUpdatePropertyNeeded() {
        return getInceptorTableAttributes().isUpdatePropertyNeeded();
    }

    public boolean isVirtualColumnSupported() {
        return getInceptorTableAttributes().isVirtualColumnSupported();
    }

    public boolean isCRUDSupported() {
        return getInceptorTableAttributes().isCRUDSupported();
    }

    public boolean isLocalModeEnabled() {
        return getInceptorTableAttributes().isLocalModeEnabled();
    }

    public boolean isLockNeededInCrud() {
        return getInceptorTableAttributes().isLockNeededInCrud();
    }

    public boolean isVirtualRowIdInCrudNeeded() {
        return getInceptorTableAttributes().isVirtualRowIdInCrudNeeded();
    }

    public boolean isSortByInCrudNeeded() {
        return getInceptorTableAttributes().isSortByInCrudNeeded();
    }

    public boolean isEnforceBucketing() {
        return getInceptorTableAttributes().isEnforceBucketing();
    }

    public boolean isCreateGlobalIndexSupported() {
        return getInceptorTableAttributes().isCreateGlobalIndexSupported();
    }

    public boolean isCreateLocalIndexSupported() {
        return getInceptorTableAttributes().isCreateLocalIndexSupported();
    }

    public List<VirtualColumn> getVirtualColumns() throws SemanticException {
        return getInceptorTableAttributes().getVirtualColumns();
    }

    public boolean isGeoSupported() {
        return getInceptorTableAttributes().isGEOSupported();
    }

    public List<VirtualColumn> getAllVirtualColumns(HiveConf hiveConf) throws SemanticException {
        List<VirtualColumn> virtualColumns = VirtualColumn.getRegistry((Configuration)hiveConf);
        if (getVirtualColumns() != null)
            virtualColumns.addAll(getVirtualColumns());
        return virtualColumns;
    }

    public PrimaryKeyInfo getPrimaryKeyInfo() throws HiveException {
        if (this.pkInfo == null) {
            Hive db = Hive.get();
            this.pkInfo = db.getPrimaryKeys(getDbName(), getTableName());
        }
        return this.pkInfo;
    }

    public void setPrimaryKeyInfo(PrimaryKeyInfo pkInfo) throws HiveException {
        this.pkInfo = pkInfo;
    }

    public String getIntervalValue() {
        return this.tTable.getIntervalValue();
    }

    public boolean isPushFilterToStorageHandlerSupported() {
        return getInceptorTableAttributes().isPushFilterToStorageHandlerSupported();
    }

    public boolean isBulkloadEnabled() {
        if (getSd().getSerdeInfo().getSerializationLib().contains("HyperbaseSerDe"))
            return true;
        return getInceptorTableAttributes().isBulkloadEnabled();
    }

    public boolean isTransactional() {
        return getInceptorTableAttributes().isTransactional();
    }

    public StorageType getStorageType() {
        return getInceptorTableAttributes().getStorageType();
    }

    public StorageDDLSupportedOperation getStorageDDLSupportedOperation() {
        return getInceptorTableAttributes().getStorageDDLSupportedOperation();
    }

    public String getStorageName() {
        return getInceptorTableAttributes().getStorageName();
    }

    public <T extends org.apache.hadoop.hive.ql.stargate.external.ExternalDDLAction> boolean isSupportFor(T action) {
        HiveStorageHandler hiveStorageHandler = getStorageHandler();
        if (null == hiveStorageHandler)
            return false;
        if (hiveStorageHandler instanceof StargateStorageHandler)
            return ((StargateStorageHandler)hiveStorageHandler).<T>getAction(action).isSupported();
        return false;
    }
}

