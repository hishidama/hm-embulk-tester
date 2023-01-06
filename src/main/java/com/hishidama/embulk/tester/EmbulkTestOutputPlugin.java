package com.hishidama.embulk.tester;

import java.text.MessageFormat;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.modules.ZoneIdModule;

public class EmbulkTestOutputPlugin implements OutputPlugin {

	public static final String TYPE = "EmbulkTestOutputPlugin";

	public static class OutputRecord {
		private Map<String, Object> map = new LinkedHashMap<>();

		public void set(String name, Object value) {
			map.put(name, value);
		}

		public String getAsString(String name) {
			try {
				return (String) map.get(name);
			} catch (Exception e) {
				throw new RuntimeException(MessageFormat.format("name={0}", name), e);
			}
		}

		public Long getAsLong(String name) {
			try {
				return (Long) map.get(name);
			} catch (Exception e) {
				throw new RuntimeException(MessageFormat.format("name={0}", name), e);
			}
		}

		public Double getAsDouble(String name) {
			try {
				return (Double) map.get(name);
			} catch (Exception e) {
				throw new RuntimeException(MessageFormat.format("name={0}", name), e);
			}
		}

		public Boolean getAsBoolean(String name) {
			try {
				return (Boolean) map.get(name);
			} catch (Exception e) {
				throw new RuntimeException(MessageFormat.format("name={0}", name), e);
			}
		}

		public Instant getAsTimestamp(String name) {
			try {
				return (Instant) map.get(name);
			} catch (Exception e) {
				throw new RuntimeException(MessageFormat.format("name={0}", name), e);
			}
		}

		@Override
		public String toString() {
			return map.toString();
		}
	}

	/**
	 * TODO staticフィールドを使わずに{@link EmbulkPluginTester#runInput(ConfigSource)}に渡したい
	 */
	public static final List<OutputRecord> resultList = new CopyOnWriteArrayList<>();

	protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules()
			.addModule(ZoneIdModule.withLegacyNames()).build();

	protected static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
	protected static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

	public interface PluginTask extends Task {
	}

	public static void clearResult() {
		resultList.clear();
	}

	public static List<OutputRecord> getResult() {
		return resultList;
	}

	@Override
	public ConfigDiff transaction(ConfigSource config, Schema schema, int taskCount, OutputPlugin.Control control) {
		final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
		return resume(task.toTaskSource(), schema, taskCount, control);
	}

	@Override
	public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, OutputPlugin.Control control) {
		control.run(taskSource);
		return CONFIG_MAPPER_FACTORY.newConfigDiff();
	}

	@Override
	public void cleanup(TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports) {
	}

	@Override
	public TransactionalPageOutput open(TaskSource taskSource, final Schema schema, int taskIndex) {
		return new TransactionalPageOutput() {
			private final PageReader reader = Exec.getPageReader(schema);

			@Override
			public void add(Page page) {
				reader.setPage(page);
				while (reader.nextRecord()) {
					final OutputRecord record = new OutputRecord();
					for (Column column : schema.getColumns()) {
						column.visit(new ColumnVisitor() {
							@Override
							public void timestampColumn(Column column) {
								if (reader.isNull(column)) {
									record.set(column.getName(), null);
									return;
								}
								record.set(column.getName(), reader.getTimestampInstant(column));
							}

							@Override
							public void stringColumn(Column column) {
								if (reader.isNull(column)) {
									record.set(column.getName(), null);
									return;
								}
								record.set(column.getName(), reader.getString(column));
							}

							@Override
							public void longColumn(Column column) {
								if (reader.isNull(column)) {
									record.set(column.getName(), null);
									return;
								}
								record.set(column.getName(), reader.getLong(column));
							}

							@Override
							public void doubleColumn(Column column) {
								if (reader.isNull(column)) {
									record.set(column.getName(), null);
									return;
								}
								record.set(column.getName(), reader.getDouble(column));
							}

							@Override
							public void booleanColumn(Column column) {
								if (reader.isNull(column)) {
									record.set(column.getName(), null);
									return;
								}
								record.set(column.getName(), reader.getBoolean(column));
							}

							@Override
							public void jsonColumn(Column column) {
								throw new UnsupportedOperationException();
							}
						});
					}
					resultList.add(record);
				}
			}

			@Override
			public void finish() {
			}

			@Override
			public void close() {
				reader.close();
			}

			@Override
			public void abort() {
			}

			@Override
			public TaskReport commit() {
				return CONFIG_MAPPER_FACTORY.newTaskReport();
			}
		};
	}
}
