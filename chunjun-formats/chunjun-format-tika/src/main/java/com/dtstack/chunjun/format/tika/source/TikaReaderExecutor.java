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

package com.dtstack.chunjun.format.tika.source;

import com.dtstack.chunjun.format.tika.common.TikaData;
import com.dtstack.chunjun.format.tika.config.TikaReadConfig;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ContentHandlerDecorator;
import org.xml.sax.ContentHandler;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static com.dtstack.chunjun.format.tika.config.TikaReadConfig.ORIGINAL_FILENAME;

public class TikaReaderExecutor implements Runnable {

    private TikaReadConfig tikaReadConfig;
    private BlockingQueue<TikaData> queue;
    private Map<String, String> metaData = new HashMap<>();
    private String metaDataString;
    private String originalFilename;
    private InputStream in;

    public TikaReaderExecutor(
            TikaReadConfig tikaReadConfig,
            BlockingQueue<TikaData> queue,
            InputStream in,
            String originalFilename) {
        this.tikaReadConfig = tikaReadConfig;
        this.queue = queue;
        this.in = in;
        this.originalFilename = originalFilename;
    }

    @Override
    public void run() {
        // 抽取文档内容
        Parser parser = new AutoDetectParser();
        // 内容处理器，用来收集结果，Tika可以将解析结果包装成XHTML SAX
        // event进行分发，通过ContentHandler处理这些event就可以得到文本内容和其他有用的信息
        ContentHandler contentHandler = new BodyContentHandler(-1);
        // 元数据，既是输入也是输出，可以将文件名或者可能的文件类型传入，tika解析时可以根据这些信息判断文件类型，
        // 再调用相应的解析器进行处理；另外，tika也会将一些额外的信息保存到Metadata中，如文件修改日期，作者，编辑工具等
        Metadata metadata = new Metadata();
        // 解析上下文，用来控制解析过程，比如是否提取Office文档里面的宏等
        ParseContext context = new ParseContext();

        // tika官方文档提供的分块处理思路, 但是测试发现比如同类型word(doc)两个文件，有的可以正常分块，有的不能分块。
        // 还有txt类型文件未能分块读取, pdf文件暂时测试。
        // 因此暂时不建议使用
        final List<String> chunks = new ArrayList<>();
        chunks.add("");
        ContentHandlerDecorator trunkHandler =
                new ContentHandlerDecorator() {
                    @Override
                    public void characters(char[] ch, int start, int length) {
                        String chunkContent = "";
                        String lastChunk = chunks.get(chunks.size() - 1);
                        String thisStr = new String(ch, start, length);
                        if (lastChunk.length() + length > tikaReadConfig.getChunkSize()) {
                            chunks.add(thisStr);
                            chunkContent = thisStr;
                        } else {
                            String chunkString = lastChunk + thisStr;
                            chunks.set(chunks.size() - 1, chunkString);
                            if (StringUtils.isNotBlank(chunkString)) {
                                chunkContent = chunkString;
                            }
                        }
                        if (metaData.isEmpty()) {
                            for (String name : metadata.names()) {
                                metaData.put(name, metadata.get(name));
                            }
                            metaData.put(ORIGINAL_FILENAME, originalFilename);
                            metaDataString = GsonUtil.GSON.toJson(metaData);
                        }
                        if (StringUtils.isNotBlank(chunkContent)) {
                            try {
                                queue.put(
                                        new TikaData(
                                                new String[] {chunkContent, metaDataString},
                                                false));
                            } catch (InterruptedException e) {
                                throw new RuntimeException(
                                        "because the current thread was interrupted, adding data to the queue failed",
                                        e);
                            }
                        }
                    }
                };

        // InputStream in 待解析的文档，以字节流形式传入，可以避免tika占用太多内存
        try (BufferedInputStream bufferedInputStream = new BufferedInputStream(in)) {
            // 如何想要使用官方的分块处理方式，需要将contentHandler替换成trunkHandler
            parser.parse(bufferedInputStream, contentHandler, metadata, context);
            String content = contentHandler.toString();
            for (String name : metadata.names()) {
                metaData.put(name, metadata.get(name));
            }
            metaData.put(ORIGINAL_FILENAME, originalFilename);
            metaDataString = GsonUtil.GSON.toJson(metaData);
            if (tikaReadConfig.getChunkSize() > 0) {
                // 对整个抽取出来的内容进行分块、内容重复度处理
                List<String> chunkList =
                        getChunkList(
                                content,
                                tikaReadConfig.getChunkSize(),
                                tikaReadConfig.getOverlapRatio());
                for (String chunk : chunkList) {
                    queue.put(new TikaData(new String[] {chunk, metaDataString}, false));
                }
            } else {
                queue.put(new TikaData(new String[] {content, metaDataString}, false));
            }
            queue.put(new TikaData(null, true));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getChunkList(String content, int chunkSize, int overlapRatio) {
        List<String> chunks = new ArrayList<>();
        int length = content.length();
        int startIndex = 0;
        int step = chunkSize;
        int endIndex = startIndex + step;
        int increment = step * overlapRatio / 100;
        if (step >= length) {
            chunks.add(content);
        } else {
            while (endIndex <= length) {
                // 确保截取的字符串不会超过原始字符串的长度
                if (startIndex + step > length) {
                    endIndex = length;
                }
                String substring = content.substring(startIndex, endIndex);
                chunks.add(substring);
                // 更新起始和结束位置
                startIndex = endIndex - increment;
                endIndex = startIndex + step;
            }
            if (endIndex > length && startIndex + increment < length) {
                String substring = content.substring(startIndex, length);
                chunks.add(substring);
            }
        }
        return chunks;
    }
}
