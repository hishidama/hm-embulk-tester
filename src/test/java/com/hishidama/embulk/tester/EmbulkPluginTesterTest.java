package com.hishidama.embulk.tester;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.embulk.config.ConfigSource;
import org.junit.Test;

import com.hishidama.embulk.tester.EmbulkTestOutputPlugin.OutputRecord;

public class EmbulkPluginTesterTest {

	@Test
	public void runInput() {
		try (EmbulkPluginTester tester = new EmbulkPluginTester()) {
			ConfigSource in = tester.newConfigSource(EmbulkTestFileInputPlugin.TYPE);
			in.set("textList", Arrays.asList("abc", "def"));
			in.set("taskSize", 1);
			EmbulkTestParserConfig parser = tester.newParserConfig("csv");
			String columnName = "text";
			parser.addColumn(columnName, "string");
			in.set("parser", parser);

			List<OutputRecord> result = tester.runInput(in);
			assertEquals(2, result.size());
			assertEquals("abc", result.get(0).getAsString(columnName));
			assertEquals("def", result.get(1).getAsString(columnName));
		}
	}

	@Test
	public void runParser() {
		try (EmbulkPluginTester tester = new EmbulkPluginTester()) {
			EmbulkTestParserConfig parser = tester.newParserConfig("csv");
			String columnName = "text";
			parser.addColumn(columnName, "string");

			List<String> list = Arrays.asList("abc", "def");
			List<OutputRecord> result = tester.runParser(list, parser);
			assertEquals(2, result.size());
			assertEquals("abc", result.get(0).getAsString(columnName));
			assertEquals("def", result.get(1).getAsString(columnName));
		}
	}

	@Test
	public void runOutput() {
		try (EmbulkPluginTester tester = new EmbulkPluginTester()) {
			EmbulkTestParserConfig parser = tester.newParserConfig("csv");
			String columnName = "text";
			parser.addColumn(columnName, "string");

			ConfigSource out = tester.newConfigSource(EmbulkTestOutputPlugin.TYPE);

			List<String> list = Arrays.asList("abc", "def");
			EmbulkTestOutputPlugin.clearResult();
			tester.runOutput(list, parser, out);
			List<OutputRecord> result = EmbulkTestOutputPlugin.getResult();
			assertEquals(2, result.size());
			assertEquals("abc", result.get(0).getAsString(columnName));
			assertEquals("def", result.get(1).getAsString(columnName));
		}
	}
}
