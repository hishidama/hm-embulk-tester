package com.hishidama.embulk.tester;

import java.io.Closeable;
import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.embulk.EmbulkEmbed;
import org.embulk.EmbulkEmbed.Bootstrap;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.DecoderPlugin;
import org.embulk.spi.EncoderPlugin;
import org.embulk.spi.ExecutorPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.GuessPlugin;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;

import com.hishidama.embulk.tester.EmbulkTestOutputPlugin.OutputRecord;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/test/java/org/embulk/output/jdbc/tester/EmbulkPluginTester.java
public class EmbulkPluginTester implements Closeable {
	private static class PluginDefinition {
		@SuppressWarnings("unused")
		public final Class<?> iface;
		public final String name;
		public final Class<?> impl;

		public PluginDefinition(Class<?> iface, String name, Class<?> impl) {
			this.iface = iface;
			this.name = name;
			this.impl = impl;
		}

		@SuppressWarnings("unchecked")
		public <T> Class<T> impl() {
			return (Class<T>) impl;
		}
	}

	private final Map<Class<?>, Map<String, PluginDefinition>> plugins = new HashMap<>();
	private EmbulkEmbed embulk;
	private ConfigLoader configLoader;
	private int inputTaskSize = 1;

	public EmbulkPluginTester() {
	}

	public <T> EmbulkPluginTester(Class<T> iface, String name, Class<? extends T> impl) {
		addPlugin(iface, name, impl);
	}

	public <T> void addPlugin(Class<T> iface, String name, Class<? extends T> impl) {
		if (!iface.isAssignableFrom(impl)) {
			throw new IllegalArgumentException(
					MessageFormat.format("name={1}. {2} is not {0}", iface.getSimpleName(), name, impl.getName()));
		}
		plugins.computeIfAbsent(iface, k -> new LinkedHashMap<>()).put(name, new PluginDefinition(iface, name, impl));
	}

	public Collection<PluginDefinition> getPlugins(Class<?> iface) {
		return plugins.getOrDefault(iface, Collections.emptyMap()).values();
	}

	public void addFileInputPlugin(String name, Class<? extends FileInputPlugin> impl) {
		addPlugin(FileInputPlugin.class, name, impl);
	}

	public void addParserPlugin(String name, Class<? extends ParserPlugin> impl) {
		addPlugin(ParserPlugin.class, name, impl);
	}

	public void addOutputPlugin(String name, Class<? extends OutputPlugin> impl) {
		addPlugin(OutputPlugin.class, name, impl);
	}

	public void setInputTaskSize(int taskSize) {
		this.inputTaskSize = taskSize;
	}

	protected synchronized EmbulkEmbed getEmbulkEmbed() {
		if (this.embulk == null) {
			addDefaultPlugin();

			Bootstrap bootstrap = new EmbulkEmbed.Bootstrap();
			for (PluginDefinition plugin : getPlugins(DecoderPlugin.class)) {
				bootstrap.builtinDecoderPlugin(plugin.name, plugin.impl());
			}
			for (PluginDefinition plugin : getPlugins(EncoderPlugin.class)) {
				bootstrap.builtinEncoderPlugin(plugin.name, plugin.impl());
			}
			for (PluginDefinition plugin : getPlugins(ExecutorPlugin.class)) {
				bootstrap.builtinExecutorPlugin(plugin.name, plugin.impl());
			}
			for (PluginDefinition plugin : getPlugins(FileInputPlugin.class)) {
				bootstrap.builtinFileInputPlugin(plugin.name, plugin.impl());
			}
			for (PluginDefinition plugin : getPlugins(FileOutputPlugin.class)) {
				bootstrap.builtinFileOutputPlugin(plugin.name, plugin.impl());
			}
			for (PluginDefinition plugin : getPlugins(FilterPlugin.class)) {
				bootstrap.builtinFilterPlugin(plugin.name, plugin.impl());
			}
			for (PluginDefinition plugin : getPlugins(FormatterPlugin.class)) {
				bootstrap.builtinFormatterPlugin(plugin.name, plugin.impl());
			}
			for (PluginDefinition plugin : getPlugins(GuessPlugin.class)) {
				bootstrap.builtinGuessPlugin(plugin.name, plugin.impl());
			}
			for (PluginDefinition plugin : getPlugins(InputPlugin.class)) {
				bootstrap.builtinInputPlugin(plugin.name, plugin.impl());
			}
			for (PluginDefinition plugin : getPlugins(OutputPlugin.class)) {
				bootstrap.builtinOutputPlugin(plugin.name, plugin.impl());
			}
			for (PluginDefinition plugin : getPlugins(ParserPlugin.class)) {
				bootstrap.builtinParserPlugin(plugin.name, plugin.impl());
			}
			this.embulk = bootstrap.initialize();
		}
		return this.embulk;
	}

	protected void addDefaultPlugin() {
		addFileInputPlugin(EmbulkTestFileInputPlugin.TYPE, EmbulkTestFileInputPlugin.class);
		addParserPlugin("csv", CsvParserPlugin.class);
		addOutputPlugin(EmbulkTestOutputPlugin.TYPE, EmbulkTestOutputPlugin.class);
	}

	public synchronized ConfigLoader getConfigLoader() {
		if (this.configLoader == null) {
			this.configLoader = getEmbulkEmbed().newConfigLoader();
		}
		return this.configLoader;
	}

	public ConfigSource newConfigSource(String type) {
		ConfigSource config = newConfigSource();
		config.set("type", type);
		return config;
	}

	public ConfigSource newConfigSource() {
		return getConfigLoader().newConfigSource();
	}

	public EmbulkTestParserConfig newParserConfig(String type) {
		EmbulkTestParserConfig parser = new EmbulkTestParserConfig();
		parser.setType(type);
		return parser;
	}

	public List<OutputRecord> runParser(URL inFile, EmbulkTestParserConfig parser) {
		Path file;
		try {
			file = Paths.get(inFile.toURI());
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
		return runParser(file, parser);
	}

	public List<OutputRecord> runParser(File inFile, EmbulkTestParserConfig parser) {
		return runParser(inFile.toPath(), parser);
	}

	public List<OutputRecord> runParser(Path inFile, EmbulkTestParserConfig parser) {
		ConfigSource in = newConfigSource("file");
		in.set("path_prefix", inFile.toAbsolutePath().toString());
		in.set("parser", parser);
		return runInput(in);
	}

	public List<OutputRecord> runParser(List<String> inList, EmbulkTestParserConfig parser) {
		ConfigSource in = newConfigSource(EmbulkTestFileInputPlugin.TYPE);
		in.set("textList", inList);
		in.set("taskSize", this.inputTaskSize);
		in.set("parser", parser);

		return runInput(in);
	}

	public List<OutputRecord> runInput(ConfigSource in) {
		ConfigSource out = newConfigSource(EmbulkTestOutputPlugin.TYPE);

		EmbulkTestOutputPlugin.clearResult();
		run(in, out);
		return EmbulkTestOutputPlugin.getResult();
	}

	public void runOutput(List<String> inList, EmbulkTestParserConfig parser, ConfigSource out) {
		ConfigSource in = newConfigSource(EmbulkTestFileInputPlugin.TYPE);
		in.set("textList", inList);
		in.set("taskSize", this.inputTaskSize);
		in.set("parser", parser);

		run(in, out);
	}

	public void run(ConfigSource in, ConfigSource out) {
		ConfigSource config = newConfigSource();
		config.set("in", in);
		config.set("out", out);
		run(config);
	}

	public void run(ConfigSource config) {
		getEmbulkEmbed().run(config);
	}

	@Override
	public void close() {
		if (this.embulk != null) {
//			embulk.destroy(); // unsupported
			this.embulk = null;
		}
	}
}
