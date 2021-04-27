package com.dtstack.flinkx.s3.format;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.s3.S3Util;
import com.dtstack.flinkx.s3.S3Config;
import com.dtstack.flinkx.util.RangeSplitUtil;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author jier@dtstack.com
 */
public class S3InputFormat extends BaseRichInputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(S3InputFormat.class);

    private List<String> objects = new ArrayList<>();


    private S3Config s3Config;
    protected List<MetaColumn> metaColumns;
    private Iterator<String> splits;

    private transient AmazonS3 amazonS3;
    private transient LineOffsetBufferedReader br;
    private transient String line;
    private transient String currentObject;
    private transient Map<String, Long> offsetMap;

//    private int count = 0;


    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        amazonS3 = S3Util.initS3(s3Config);
    }

    @Override
    protected void openInternal(InputSplit split) throws IOException {
        S3InputSplit inputSplit = (S3InputSplit) split;
        List<String> splitsList = inputSplit.getSplits();
        LinkedList<String> result = new LinkedList<>();
        if (formatState != null && formatState.getState() != null && formatState.getState() instanceof Map) {
            offsetMap = (Map) formatState.getState();
            for (int i = 0; i < splitsList.size(); i++) {
                String object = splitsList.get(i);
                if (object.hashCode() % inputSplit.getTotalNumberOfSplits() == indexOfSubTask) {
                    if (offsetMap.containsKey(object) && 0 < offsetMap.get(object)) {
                        result.addFirst(object);
                    } else if (!offsetMap.containsKey(object) || 0 == offsetMap.get(object)) {
                        result.add(object);
                    }
                }
            }
        } else {
            offsetMap = new ConcurrentHashMap<>(inputSplit.getSplits().size());
            for (int i = 0; i < splitsList.size(); i++) {
                String object = splitsList.get(i);
                if (object.hashCode() % inputSplit.getTotalNumberOfSplits() == indexOfSubTask) {
                    result.add(object);
                }
            }
        }
        splits = result.iterator();
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        //todo 使用动态规划划分切片，使每块切片的文件总大小近似相当，解决按照 hash 分配容易造成数据倾斜的问题。

        S3InputSplit[] splits = new S3InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new S3InputSplit(indexOfSubTask, minNumSplits, objects);
        }
        return splits;
    }

    @Override
    protected Row nextRecordInternal(Row row) {
        /*LOG.info("this count is {}", count);
        if (count == 100) {
            System.out.println(1 / 0);
        }*/
        String[] fields = line.split(s3Config.getFieldDelimiter());
        if (metaColumns.size() == 1 && ConstantValue.STAR_SYMBOL.equals(metaColumns.get(0).getName())) {
            row = new Row(fields.length);
            for (int i = 0; i < fields.length; i++) {
                row.setField(i, fields[i]);
            }
        } else {
            row = new Row(metaColumns.size());
            for (int i = 0; i < metaColumns.size(); i++) {
                MetaColumn metaColumn = metaColumns.get(i);

                Object value = null;
                if (metaColumn.getIndex() != null && metaColumn.getIndex() < fields.length) {
                    value = fields[metaColumn.getIndex()];
                    if (((String) value).length() == 0) {
                        value = metaColumn.getValue();
                    }
                } else if (metaColumn.getValue() != null) {
                    value = metaColumn.getValue();
                }

                if (value != null) {
                    value = StringUtil.string2col(String.valueOf(value), metaColumn.getType(), metaColumn.getTimeFormat());
                }

                row.setField(i, value);
            }
        }
        if (restoreConfig.isRestore()) {
            offsetMap.replace(currentObject, br.getOffset());
        }
        /*count++;
        try {
            TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
        return row;
    }

    @Override
    protected void closeInternal() throws IOException {
        if (br != null) {
            br.close();
            br = null;
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return reachedEndWithoutCheckState();
    }

    public boolean reachedEndWithoutCheckState() throws IOException {
        //br 为空，说明需要读新文件
        if (br == null) {
            if (splits.hasNext()) {
                //若还有新文件，则读新文件
                currentObject = splits.next();
                GetObjectRequest rangeObjectRequest = new GetObjectRequest(s3Config.getBucket(), currentObject);

                if (offsetMap.containsKey(currentObject) && 0 < offsetMap.get(currentObject)) {
                    //若已经读过该文件且还没读取完成，则继续读
                    long offset = offsetMap.get(currentObject);
                    rangeObjectRequest.setRange(offset);
                    S3Object o = amazonS3.getObject(rangeObjectRequest);
                    S3ObjectInputStream s3is = o.getObjectContent();
                    br = new LineOffsetBufferedReader(new InputStreamReader(
                            s3is, s3Config.getEncoding()), offset);
                    offsetMap.put(currentObject, offset);
                }else {
                    //没读过该文件，或者读过但偏移量为0
                    S3Object o = amazonS3.getObject(rangeObjectRequest);
                    S3ObjectInputStream s3is = o.getObjectContent();
                    br = new LineOffsetBufferedReader(new InputStreamReader(
                            s3is, s3Config.getEncoding()));
                    if(s3Config.isFirstLineHeader()){
                        br.readLine();
                        offsetMap.put(currentObject, br.getOffset());
                    }
                }
            } else {
                //若没有新文件，则读完所有文件
                return true;
            }
        }
        //br不为空且正在读文件且 splits 还有下一个文件
        line = br.readLine();
        if (line != null) {
            //还没读取完本次读取的文件
            return false;
        } else {
            //读取完本次读取的文件，关闭 br 并置空
            br.close();
            br = null;
            offsetMap.replace(currentObject, -1L);
            //尝试去读新文件
            return reachedEndWithoutCheckState();
        }
    }


    @Override
    public FormatState getFormatState() {
        super.getFormatState();
        if (formatState != null && offsetMap != null && !offsetMap.isEmpty()) {
            formatState.setState(offsetMap);
        }
        return formatState;
    }

    public S3Config getS3Config() {
        return s3Config;
    }

    public void setS3Config(S3Config s3Config) {
        this.s3Config = s3Config;
    }

    public void setObjects(List<String> objects) {
        this.objects = objects;
    }

    public void setMetaColumn(List<MetaColumn> metaColumns) {
        this.metaColumns = metaColumns;
    }
}
