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

package com.dtstack.chunjun.connector.s3.source;

import com.dtstack.chunjun.conf.RestoreConf;
import com.dtstack.chunjun.connector.s3.conf.S3Conf;
import com.dtstack.chunjun.connector.s3.util.ReaderUtil;
import com.dtstack.chunjun.connector.s3.util.S3SimpleObject;
import com.dtstack.chunjun.connector.s3.util.S3Util;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.ReadRecordException;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** @author jier */
public class S3InputFormat extends BaseRichInputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(S3InputFormat.class);

    private static final long serialVersionUID = -3217513386563100062L;

    private S3Conf s3Conf;
    private Iterator<String> splits;

    private transient AmazonS3 amazonS3;

    private transient String currentObject;
    private transient Map<String, Long> offsetMap;

    private transient ReaderUtil readerUtil = null;

    private RestoreConf restoreConf;

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
    }

    @Override
    protected void openInternal(InputSplit split) {
        amazonS3 = S3Util.getS3Client(s3Conf);
        S3InputSplit inputSplit = (S3InputSplit) split;
        List<String> splitsList = inputSplit.getSplits();
        LinkedList<String> result = new LinkedList<>();
        if (restoreConf.isRestore()
                && formatState != null
                && formatState.getState() != null
                && formatState.getState() instanceof Map) {
            offsetMap = (Map) formatState.getState();
            for (int i = 0; i < splitsList.size(); i++) {
                String object = splitsList.get(i);
                if (i % inputSplit.getTotalNumberOfSplits() == indexOfSubTask) {
                    if (offsetMap.containsKey(object) && 0 < offsetMap.get(object)) {
                        result.addFirst(object);
                    } else if (!offsetMap.containsKey(object) || 0 == offsetMap.get(object)) {
                        result.add(object);
                    }
                }
            }
        } else {
            if (restoreConf.isRestore()) {
                offsetMap = new ConcurrentHashMap<>(inputSplit.getSplits().size());
            }
            for (int i = 0; i < splitsList.size(); i++) {
                String object = splitsList.get(i);
                if (i % inputSplit.getTotalNumberOfSplits() == inputSplit.getSplitNumber()) {
                    result.add(object);
                }
            }
        }
        splits = result.iterator();
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) {
        List<S3SimpleObject> objects = resolveObjects();
        if (objects.isEmpty()) {
            throw new ChunJunRuntimeException(
                    "No objects found in bucket: "
                            + s3Conf.getBucket()
                            + "，objects: "
                            + s3Conf.getObjects());
        }
        LOG.info("read file {}", GsonUtil.GSON.toJson(objects));
        List<String> keys = new ArrayList<>();
        for (S3SimpleObject object : objects) {
            keys.add(object.getKey());
        }
        S3InputSplit[] splits = new S3InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new S3InputSplit(i, minNumSplits, keys);
        }
        return splits;
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        String[] fields;
        try {
            fields = readerUtil.getValues();
            rowData = rowConverter.toInternal(fields);
        } catch (IOException e) {
            throw new ChunJunRuntimeException(e);
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, rowData);
        }
        if (restoreConf.isRestore()) {
            offsetMap.replace(currentObject, readerUtil.getNextOffset());
        }
        return rowData;
    }

    @Override
    protected void closeInternal() {
        if (amazonS3 != null) {
            amazonS3.shutdown();
            amazonS3 = null;
        }
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
        // br is empty, indicating that a new file needs to be read
        if (readerUtil == null) {
            if (splits.hasNext()) {
                // If there is a new file, read the new file
                currentObject = splits.next();
                GetObjectRequest rangeObjectRequest =
                        new GetObjectRequest(s3Conf.getBucket(), currentObject);
                LOG.info("Current read file {}", currentObject);
                if (restoreConf.isRestore()
                        && offsetMap.containsKey(currentObject)
                        && 0 <= offsetMap.get(currentObject)) {
                    // If the breakpoint resume is turned on, it means that the file has been read
                    // but not finished, so continue reading
                    long offset = offsetMap.getOrDefault(currentObject, 0L);
                    rangeObjectRequest.setRange(offset);
                    S3Object o = amazonS3.getObject(rangeObjectRequest);

                    S3ObjectInputStream s3is = o.getObjectContent();
                    readerUtil =
                            new ReaderUtil(
                                    new InputStreamReader(s3is, s3Conf.getEncoding()),
                                    s3Conf.getFieldDelimiter(),
                                    offset,
                                    s3Conf.isSafetySwitch());
                    offsetMap.put(currentObject, offset);
                } else {
                    // The resumable upload is not enabled or the resumable upload is enabled but
                    // the file has not been read
                    S3Object o = amazonS3.getObject(rangeObjectRequest);
                    S3ObjectInputStream s3is = o.getObjectContent();
                    readerUtil =
                            new ReaderUtil(
                                    new InputStreamReader(s3is, s3Conf.getEncoding()),
                                    s3Conf.getFieldDelimiter(),
                                    0L,
                                    s3Conf.isSafetySwitch());
                    if (s3Conf.isFirstLineHeader()) {
                        readerUtil.readHeaders();
                    }
                    if (restoreConf.isRestore()) {
                        offsetMap.put(currentObject, readerUtil.getNextOffset());
                    }
                }
            } else {
                // All files have been read
                return true;
            }
        }
        if (readerUtil.readRecord()) {
            // The file has not been read
            return false;
        } else {
            // After reading the file read this time, close br and clear it
            readerUtil.close();
            readerUtil = null;
            if (restoreConf.isRestore()) {
                offsetMap.replace(currentObject, -1L);
            }
            // try to read the new file
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

    public List<S3SimpleObject> resolveObjects() {
        String bucket = s3Conf.getBucket();
        Set<S3SimpleObject> resolved = new HashSet<>();
        AmazonS3 amazonS3 = S3Util.getS3Client(s3Conf);
        for (String key : s3Conf.getObjects()) {
            if (StringUtils.isNotBlank(key)) {
                if (key.endsWith(".*")) {
                    // End with .*, indicating that the object is prefixed
                    String prefix = key.substring(0, key.indexOf(".*"));
                    List<String> subObjects;
                    if (s3Conf.isUseV2()) {
                        subObjects =
                                S3Util.listObjectsKeyByPrefix(
                                        amazonS3, bucket, prefix, s3Conf.getFetchSize());
                    } else {
                        subObjects =
                                S3Util.listObjectsByv1(
                                        amazonS3, bucket, prefix, s3Conf.getFetchSize());
                    }
                    for (String subObject : subObjects) {
                        S3SimpleObject s3SimpleObject = S3Util.getS3SimpleObject(subObject);
                        resolved.add(s3SimpleObject);
                    }
                } else if (S3Util.doesObjectExist(amazonS3, bucket, key)) {
                    // Exact query and object exists
                    S3SimpleObject s3SimpleObject = S3Util.getS3SimpleObject(key);
                    resolved.add(s3SimpleObject);
                }
            }
        }
        List<S3SimpleObject> distinct = new ArrayList<>(resolved);
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "match object is[{}]",
                    distinct.stream().map(S3SimpleObject::getKey).collect(Collectors.joining(",")));
        }
        return distinct;
    }

    public S3Conf getS3Conf() {
        return s3Conf;
    }

    public void setS3Conf(S3Conf s3Conf) {
        this.s3Conf = s3Conf;
    }

    public RestoreConf getRestoreConf() {
        return restoreConf;
    }

    public void setRestoreConf(RestoreConf restoreConf) {
        this.restoreConf = restoreConf;
    }
}
