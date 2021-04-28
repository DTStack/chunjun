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

package com.dtstack.flinkx.s3.format;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.s3.S3SimpleObject;
import com.dtstack.flinkx.s3.S3Util;
import com.dtstack.flinkx.s3.S3Config;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class S3InputFormat extends BaseRichInputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(S3InputFormat.class);

    private static final long serialVersionUID = -3217513386563100062L;

    private List<S3SimpleObject> objects = new ArrayList<>();


    private S3Config s3Config;
    protected List<MetaColumn> metaColumns;
    private Iterator<String> splits;

    private transient AmazonS3 amazonS3;

    private transient String currentObject;
    private transient Map<String, Long> offsetMap;

    private transient ReaderUtil readerUtil = null;


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
        if (restoreConfig.isRestore() && formatState != null
                && formatState.getState() != null && formatState.getState() instanceof Map) {
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
            if (restoreConfig.isRestore()) {
                offsetMap = new ConcurrentHashMap<>(inputSplit.getSplits().size());
            }
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

        List<String> keys = new ArrayList<>();
        for (S3SimpleObject object : objects) {
            keys.add(object.getKey());
        }
        S3InputSplit[] splits = new S3InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new S3InputSplit(indexOfSubTask, minNumSplits, keys);
        }
        return splits;
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        String[] fields = readerUtil.getValues();
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
            offsetMap.replace(currentObject, readerUtil.getNextOffset());
        }
        return row;
    }


    @Override
    protected void closeInternal() throws IOException {
        if (readerUtil != null) {
            readerUtil.close();
            readerUtil = null;
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return reachedEndWithoutCheckState();
    }

    public boolean reachedEndWithoutCheckState() throws IOException {
        //br 为空，说明需要读新文件
        if (readerUtil == null) {
            if (splits.hasNext()) {
                //若还有新文件，则读新文件
                currentObject = splits.next();
                GetObjectRequest rangeObjectRequest = new GetObjectRequest(s3Config.getBucket(), currentObject);

                if (restoreConfig.isRestore() && offsetMap.containsKey(currentObject) && 0 <= offsetMap.get(currentObject)) {
                    //若开启断点续传，已经读过该文件且还没读取完成，则继续读
                    long offset = offsetMap.getOrDefault(currentObject, 0L);
                    rangeObjectRequest.setRange(offset);
                    S3Object o = amazonS3.getObject(rangeObjectRequest);
                    S3ObjectInputStream s3is = o.getObjectContent();
                    readerUtil = new ReaderUtil(new InputStreamReader(
                            s3is, s3Config.getEncoding()), s3Config.getFieldDelimiter(), offset);
                    offsetMap.put(currentObject, offset);
                } else {
                    //未开启断点续传 或开启断点续传没读过
                    //（不存在已读完的文件，若存在已读完的文件且开启了断点续传功能，则在openInternal方法中不会将其加入待处理文件）
                    S3Object o = amazonS3.getObject(rangeObjectRequest);
                    S3ObjectInputStream s3is = o.getObjectContent();
                    readerUtil = new ReaderUtil(new InputStreamReader(
                            s3is, s3Config.getEncoding()), s3Config.getFieldDelimiter());
                    if (s3Config.isFirstLineHeader()) {
                        readerUtil.readHeaders();
                    }
                    if (restoreConfig.isRestore()) {
                        offsetMap.put(currentObject, readerUtil.getNextOffset());
                    }
                }
            } else {
                //若没有新文件，则读完所有文件
                return true;
            }
        }
        //br不为空且正在读文件且 splits 还有下一个文件
        if (readerUtil.readRecord()) {
            //还没读取完本次读取的文件
            return false;
        } else {
            //读取完本次读取的文件，关闭 br 并置空
            readerUtil.close();
            readerUtil = null;
            if (restoreConfig.isRestore()) {
                offsetMap.replace(currentObject, -1L);
            }
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

    public void setObjects(List<S3SimpleObject> objects) {
        this.objects = objects;
    }

    public void setMetaColumn(List<MetaColumn> metaColumns) {
        this.metaColumns = metaColumns;
    }
}
