package com.hishidama.embulk.tester;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.plugin.DefaultPluginType;
import org.embulk.spi.EncoderPlugin;
import org.embulk.spi.ExecInternal;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.util.EncodersInternal;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.modules.ZoneIdModule;
import org.embulk.util.file.OutputStreamFileOutput;
import org.embulk.util.file.OutputStreamFileOutput.Provider;

public class EmbulkTestOutputBinaryPlugin implements OutputPlugin {

    public static final String TYPE = "EmbulkTestOutputBinaryPlugin";

    /**
     * TODO staticフィールドを使わずに{@link EmbulkPluginTester#runFormatterToBinary(List, EmbulkTestParserConfig, ConfigSource)}に渡したい
     */
    private static ByteArrayOutputStream bos;

    protected static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().addModule(ZoneIdModule.withLegacyNames()).build();

    protected static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    protected static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

    // https://github.com/embulk/embulk/blob/master/embulk-core/src/main/java/org/embulk/spi/FileOutputRunner.java
    public interface RunnerTask extends Task {
        @Config("type")
        public String getType();

        @Config("encoders")
        @ConfigDefault("[]")
        public List<ConfigSource> getEncoderConfigs();

        @Config("formatter")
        public ConfigSource getFormatterConfig();

        public void setFileOutputTaskSource(TaskSource v);

        public TaskSource getFileOutputTaskSource();

        public void setEncoderTaskSources(List<TaskSource> v);

        public List<TaskSource> getEncoderTaskSources();

        public void setFormatterTaskSource(TaskSource v);

        public TaskSource getFormatterTaskSource();
    }

    public static void clearResult() {
        bos = new ByteArrayOutputStream(1024);
    }

    public static byte[] getResult() {
        checkResult();
        return bos.toByteArray();
    }

    private static void checkResult() {
        if (bos == null) {
            throw new IllegalStateException("call EmbulkTestOutputBinaryPlugin.clearResult()");
        }
    }

    protected List<EncoderPlugin> newEncoderPlugins(RunnerTask task) {
        return EncodersInternal.newEncoderPlugins(ExecInternal.sessionInternal(), task.getEncoderConfigs());
    }

    protected FormatterPlugin newFormatterPlugin(RunnerTask task) {
        ConfigSource formatterConfig = task.getFormatterConfig();
        String type = formatterConfig.get(String.class, "type");
        return ExecInternal.newPlugin(FormatterPlugin.class, DefaultPluginType.create(type));
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, Schema schema, int taskCount, OutputPlugin.Control control) {
        final RunnerTask task = loadRunnerTask(config);
        TaskSource taskSource = task.toTaskSource();
        RunnerControl runnerControl = new RunnerControl(schema, task, control);
        runnerControl.run(taskSource);
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, OutputPlugin.Control control) {
        final RunnerTask task = loadRunnerTaskFromTaskSource(taskSource);
        RunnerControl runnerControl = new RunnerControl(schema, task, control);
        runnerControl.run(taskSource);
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }

    private class RunnerControl implements FileOutputPlugin.Control {
        private final Schema schema;
        private final RunnerTask task;
        private final List<EncoderPlugin> encoderPlugins;
        private final FormatterPlugin formatterPlugin;
        private final OutputPlugin.Control nextControl;

        public RunnerControl(Schema schema, RunnerTask task, OutputPlugin.Control nextControl) {
            this.schema = schema;
            this.task = task;
            // create plugins earlier than run() to throw exceptions early
            this.encoderPlugins = newEncoderPlugins(task);
            this.formatterPlugin = newFormatterPlugin(task);
            this.nextControl = nextControl;
        }

        @Override
        public List<TaskReport> run(final TaskSource fileOutputTaskSource) {
            final List<TaskReport> taskReports = new ArrayList<TaskReport>();
            EncodersInternal.transaction(encoderPlugins, task.getEncoderConfigs(), new EncodersInternal.Control() {
                public void run(final List<TaskSource> encoderTaskSources) {
                    formatterPlugin.transaction(task.getFormatterConfig(), schema, new FormatterPlugin.Control() {
                        public void run(final TaskSource formatterTaskSource) {
                            task.setFileOutputTaskSource(fileOutputTaskSource);
                            task.setEncoderTaskSources(encoderTaskSources);
                            task.setFormatterTaskSource(formatterTaskSource);
                            taskReports.addAll(nextControl.run(task.toTaskSource()));
                        }
                    });
                }
            });
            return taskReports;
        }
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports) {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, final Schema schema, int taskIndex) {
        final RunnerTask task = loadRunnerTaskFromTaskSource(taskSource);
        FormatterPlugin formatterPlugin = newFormatterPlugin(task);
        OutputStreamFileOutput fileOutput = new OutputStreamFileOutput(new Provider() {
            private ByteArrayOutputStream buffer;

            @Override
            public OutputStream openNext() throws IOException {
                this.buffer = new ByteArrayOutputStream(1024);
                return buffer;
            }

            @Override
            public void finish() throws IOException {
                checkResult();
                byte[] result = buffer.toByteArray();
                synchronized (bos) {
                    bos.write(result);
                }
            }

            @Override
            public void close() throws IOException {
                buffer.close();
            }
        });
        PageOutput output = formatterPlugin.open(task.getFormatterTaskSource(), schema, fileOutput);
        return new TransactionalPageOutput() {
            @Override
            public void add(Page page) {
                output.add(page);
            }

            @Override
            public void finish() {
                output.finish();
            }

            @Override
            public void close() {
                output.close();
            }

            @Override
            public void abort() {
//              fileOutput.abort();
            }

            @Override
            public TaskReport commit() {
                return CONFIG_MAPPER_FACTORY.newTaskReport();
            }
        };
    }

    public static TaskSource getFileOutputTaskSource(TaskSource runnerTaskSource) {
        return TASK_MAPPER.map(runnerTaskSource, RunnerTask.class).getFileOutputTaskSource();
    }

    private static RunnerTask loadRunnerTask(final ConfigSource config) {
        return CONFIG_MAPPER.map(config, RunnerTask.class);
    }

    private static RunnerTask loadRunnerTaskFromTaskSource(final TaskSource taskSource) {
        return TASK_MAPPER.map(taskSource, RunnerTask.class);
    }
}
