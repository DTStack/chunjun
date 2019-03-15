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

package com.dtstack.flinkx.metrics.scope;

/**
 * A container for component scope formats.
 */
public final class ScopeFormats {

	private final PipelineScopeFormat pipelineScopeFormat;

	// ------------------------------------------------------------------------

	/**
	 * Creates all scope formats, based on the given scope format strings.
	 */
	private ScopeFormats() {
		this.pipelineScopeFormat = new PipelineScopeFormat("<host>.flinkx.<plugin_type>.<plugin_name>.<job_name>");
	}

	// ------------------------------------------------------------------------
	//  Accessors
	// ------------------------------------------------------------------------

	public PipelineScopeFormat getPipelineScopeFormat() {
		return pipelineScopeFormat;
	}

	// ------------------------------------------------------------------------
	//  Parsing from Config
	// ------------------------------------------------------------------------

	/**
	 * Creates the scope formats as defined in the given configuration.
	 *
	 * @return The ScopeFormats parsed from the configuration
	 */
	public static ScopeFormats fromDefault() {
		return new ScopeFormats();
	}
}
