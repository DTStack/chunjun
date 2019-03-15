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

package com.dtstack.flinkx.metrics;

import com.dtstack.flinkx.metrics.base.reporter.MetricReporter;
import com.dtstack.flinkx.metrics.groups.AbstractMetricGroup;
import com.dtstack.flinkx.metrics.groups.FrontMetricGroup;
import com.dtstack.flinkx.metrics.scope.ScopeFormats;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.View;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.runtime.metrics.ViewUpdater;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * copy from https://github.com/apache/flink
 *
 * A MetricRegistry keeps track of all registered {@link Metric Metrics}. It serves as the
 * connection between {@link com.dtstack.flinkx.metrics.base.MetricGroup MetricGroups} and {@link MetricReporter MetricReporters}.
 */
public class MetricRegistryImpl implements MetricRegistry {
	static final Logger LOG = LoggerFactory.getLogger(MetricRegistryImpl.class);

	private final Object lock = new Object();

	private List<MetricReporter> reporters;
	private ScheduledExecutorService executor;

	private final ScopeFormats scopeFormats = ScopeFormats.fromDefault();
	private final char globalDelimiter = '.';
	private final List<Character> delimiters = new ArrayList<>();

	private ViewUpdater viewUpdater;

	/**
	 * Creates a new MetricRegistry and starts the configured reporter.
	 */
	public MetricRegistryImpl(List<Map> reporterPlugins) {

		// instantiate any custom configured reporters
		this.reporters = new ArrayList<>();
		this.executor = Executors.newSingleThreadScheduledExecutor(new ExecutorThreadFactory("flinkx-MetricRegistry"));


		if (reporterPlugins.isEmpty()) {
			// no reporters defined
			// by default, don't report anything
			LOG.info("No metrics reporter configured, no metrics will be exposed/reported.");
		} else {
			// we have some reporters so
			for (Map reporterPlugin: reporterPlugins) {
				Iterator<Map.Entry<String, Map<String,Object>>> reportIt = reporterPlugin.entrySet().iterator();
				while (reportIt.hasNext()) {
					Map.Entry<String, Map<String,Object>> reportEntry = reportIt.next();
					String namedReporter = reportEntry.getKey();
					Map<String,Object> reporterConfig = reportEntry.getValue();
					final String className = MapUtils.getString(reporterConfig,"class", null);
					if (className == null) {
						LOG.error("No reporter class set for reporter " + namedReporter + ". Metrics might not be exposed/reported.");
						continue;
					}

					try {
						String configuredPeriod = MapUtils.getString(reporterConfig,"interval", null);
						TimeUnit timeunit = TimeUnit.SECONDS;
						long period = 10;

						if (configuredPeriod != null) {
							try {
								String[] interval = configuredPeriod.split(" ");
								period = Long.parseLong(interval[0]);
								timeunit = TimeUnit.valueOf(interval[1]);
							} catch (Exception e) {
								LOG.error("Cannot parse report interval from config: " + configuredPeriod +
										" - please use values like '10 SECONDS' or '500 MILLISECONDS'. " +
										"Using default reporting interval.");
							}
						}

						Class<?> reporterClass = Class.forName(className);
						MetricReporter reporterInstance = (MetricReporter) reporterClass.newInstance();

						MetricConfig metricConfig = new MetricConfig();
						addAllToProperties(reporterConfig, metricConfig);
						LOG.info("Configuring {} with {}.", reporterClass.getSimpleName(), metricConfig);
						reporterInstance.open(metricConfig);

						if (reporterInstance instanceof Scheduled) {
							LOG.info("Periodically reporting metrics in intervals of {} {} for reporter {} of type {}.", period, timeunit.name(), namedReporter, className);

							executor.scheduleWithFixedDelay(
									new MetricRegistryImpl.ReporterTask((Scheduled) reporterInstance), period, period, timeunit);
						} else {
							LOG.info("Reporting metrics for reporter {} of type {}.", namedReporter, className);
						}
						reporters.add(reporterInstance);

						String delimiterForReporter = MapUtils.getString(reporterConfig,"scope.delimiter", String.valueOf(globalDelimiter));
						if (delimiterForReporter.length() != 1) {
							LOG.warn("Failed to parse delimiter '{}' for reporter '{}', using global delimiter '{}'.", delimiterForReporter, namedReporter, globalDelimiter);
							delimiterForReporter = String.valueOf(globalDelimiter);
						}
						this.delimiters.add(delimiterForReporter.charAt(0));
					} catch (Throwable t) {
						LOG.error("Could not instantiate metrics reporter {}. Metrics might not be exposed/reported.", namedReporter, t);
					}
				}
			}
		}
	}

	private void addAllToProperties(Map<String, Object> confData, Properties props) {
		for (Map.Entry<String, Object> entry : confData.entrySet()) {
			props.put(entry.getKey(), entry.getValue());
		}
	}


	@Override
	public char getDelimiter() {
		return this.globalDelimiter;
	}

	@Override
	public char getDelimiter(int reporterIndex) {
		try {
			return delimiters.get(reporterIndex);
		} catch (IndexOutOfBoundsException e) {
			LOG.warn("Delimiter for reporter index {} not found, returning global delimiter.", reporterIndex);
			return this.globalDelimiter;
		}
	}

	@Override
	public int getNumberReporters() {
		return reporters.size();
	}

	public List<MetricReporter> getReporters() {
		return reporters;
	}

	/**
	 * Returns whether this registry has been shutdown.
	 *
	 * @return true, if this registry was shutdown, otherwise false
	 */
	public boolean isShutdown() {
		synchronized (lock) {
			return reporters == null && executor.isShutdown();
		}
	}

	/**
	 * Shuts down this registry and the associated {@link MetricReporter}.
	 */
	public void shutdown() {
		synchronized (lock) {
			if (reporters != null) {
				for (MetricReporter reporter : reporters) {
					try {
						reporter.close();
					} catch (Throwable t) {
						LOG.warn("Metrics reporter did not shut down cleanly", t);
					}
				}
				reporters = null;
			}
			shutdownExecutor();
		}
	}

	private void shutdownExecutor() {
		if (executor != null) {
			executor.shutdown();

			try {
				if (!executor.awaitTermination(1L, TimeUnit.SECONDS)) {
					executor.shutdownNow();
				}
			} catch (InterruptedException e) {
				executor.shutdownNow();
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Metrics (de)registration
	// ------------------------------------------------------------------------

	@Override
	public void register(Metric metric, String metricName, AbstractMetricGroup group) {
		synchronized (lock) {
			if (isShutdown()) {
				LOG.warn("Cannot register metric, because the MetricRegistry has already been shut down.");
			} else {
				if (reporters != null) {
					for (int i = 0; i < reporters.size(); i++) {
						MetricReporter reporter = reporters.get(i);
						try {
							if (reporter != null) {
								FrontMetricGroup front = new FrontMetricGroup<AbstractMetricGroup<?>>(i, group);
								reporter.notifyOfAddedMetric(metric, metricName, front);
							}
						} catch (Exception e) {
							LOG.warn("Error while registering metric.", e);
						}
					}
				}
				try {
					if (metric instanceof View) {
						if (viewUpdater == null) {
							viewUpdater = new ViewUpdater(executor);
						}
						viewUpdater.notifyOfAddedView((View) metric);
					}
				} catch (Exception e) {
					LOG.warn("Error while registering metric.", e);
				}
			}
		}
	}

	@Override
	public void unregister(Metric metric, String metricName, AbstractMetricGroup group) {
		synchronized (lock) {
			if (isShutdown()) {
				LOG.warn("Cannot unregister metric, because the MetricRegistry has already been shut down.");
			} else {
				if (reporters != null) {
					for (int i = 0; i < reporters.size(); i++) {
						try {
						MetricReporter reporter = reporters.get(i);
							if (reporter != null) {
								FrontMetricGroup front = new FrontMetricGroup<AbstractMetricGroup<?>>(i, group);
								reporter.notifyOfRemovedMetric(metric, metricName, front);
							}
						} catch (Exception e) {
							LOG.warn("Error while registering metric.", e);
						}
					}
				}
			}
		}
	}

	@Override
	public ScopeFormats getScopeFormats() {
		return scopeFormats;
	}


	/**
	 * This task is explicitly a static class, so that it does not hold any references to the enclosing
	 * MetricsRegistry instance.
	 *
	 * <p>This is a subtle difference, but very important: With this static class, the enclosing class instance
	 * may become garbage-collectible, whereas with an anonymous inner class, the timer thread
	 * (which is a GC root) will hold a reference via the timer task and its enclosing instance pointer.
	 * Making the MetricsRegistry garbage collectible makes the java.util.Timer garbage collectible,
	 * which acts as a fail-safe to stop the timer thread and prevents resource leaks.
	 */
	private static final class ReporterTask extends TimerTask {

		private final Scheduled reporter;

		private ReporterTask(Scheduled reporter) {
			this.reporter = reporter;
		}

		@Override
		public void run() {
			try {
				reporter.report();
			} catch (Throwable t) {
				LOG.warn("Error while reporting metrics", t);
			}
		}
	}
}
