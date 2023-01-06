package com.hishidama.embulk.tester;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.BufferImpl;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.modules.ZoneIdModule;

public class EmbulkTestFileInputPlugin implements FileInputPlugin {

	public static final String TYPE = "EmbulkTestFileInputPlugin";

	protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules()
			.addModule(ZoneIdModule.withLegacyNames()).build();

	protected static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
	protected static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

	public interface PluginTask extends Task {
		@Config("textList")
		List<String> getTextList();

		@Config("taskSize")
		int getTaskSize();
	}

	@Override
	public ConfigDiff transaction(ConfigSource config, Control control) {
		PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

		int taskCount = task.getTaskSize(); // タスク数（この数だけopenが呼ばれる）
		return resume(task.toTaskSource(), taskCount, control);
	}

	@Override
	public ConfigDiff resume(TaskSource taskSource, int taskCount, Control control) {
		control.run(taskSource, taskCount);
		return CONFIG_MAPPER_FACTORY.newConfigDiff();
	}

	@Override
	public void cleanup(TaskSource taskSource, int taskCount, List<TaskReport> successTaskReports) {
	}

	@Override
	public TransactionalFileInput open(TaskSource taskSource, int taskIndex) {
		PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);
		int taskSize = task.getTaskSize();
		List<String> textList = task.getTextList();
		int start = taskIndex * textList.size() / taskSize;
		int end = (taskIndex + 1) * textList.size() / taskSize;
		if (taskIndex == taskSize - 1) {
			end = textList.size();
		}
		List<String> list = textList.subList(start, end);

		return new TransactionalFileInput() {
			private boolean eof = false;
			private int index = 0;

			@Override
			public Buffer poll() {
				if (index < list.size()) {
					String s = list.get(index++) + "\n";
					return BufferImpl.copyOf(s.getBytes(StandardCharsets.UTF_8));
				}

				eof = true;
				return null;
			}

			@Override
			public boolean nextFile() {
				return !eof;
			}

			@Override
			public void close() {
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
