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

package com.dtstack.chunjun.connector.s3.sink;

import com.amazonaws.services.s3.model.PartETag;

import java.io.Serializable;

public class MyPartETag implements Serializable {
    private static final long serialVersionUID = 1L;

    private int partNumber;

    private String eTag;

    public MyPartETag(PartETag partETag) {
        this.partNumber = partETag.getPartNumber();
        this.eTag = partETag.getETag();
    }

    public PartETag genPartETag() {
        return new PartETag(this.partNumber, this.eTag);
    }

    public int getPartNumber() {
        return partNumber;
    }

    public void setPartNumber(int partNumber) {
        this.partNumber = partNumber;
    }

    public String geteTag() {
        return eTag;
    }

    public void seteTag(String eTag) {
        this.eTag = eTag;
    }

    @Override
    public String toString() {
        return partNumber + "$" + eTag;
    }
}
