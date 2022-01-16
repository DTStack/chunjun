/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.dtstack.flinkx.cdc.worker;

import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2021/12/13
 */
public class ChunkSplitter {

    /**
     * 创建worker任务分片
     *
     * @param tableIdentities tableIdentities
     * @param chunkNum how many chunks want.
     * @return Chunk[]
     */
    public static Chunk[] createChunk(Set<String> tableIdentities, int chunkNum) {
        final int size =
                tableIdentities.size() % chunkNum == 0
                        ? tableIdentities.size() / chunkNum
                        : tableIdentities.size() / chunkNum + 1;
        Iterable<List<String>> partition = Iterables.partition(tableIdentities, size);
        List<Chunk> chunkList = new ArrayList<>();
        int id = 0;
        for (List<String> list : partition) {
            String[] identities = list.toArray(new String[0]);
            Chunk chunk = new Chunk(id++, identities);
            chunkList.add(chunk);
        }
        return chunkList.toArray(new Chunk[0]);
    }
}
