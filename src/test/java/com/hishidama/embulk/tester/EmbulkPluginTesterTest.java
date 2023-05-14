package com.hishidama.embulk.tester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
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

            List<String> inList = Arrays.asList("abc", "def");
            List<OutputRecord> result = tester.runParser(inList, parser);
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

            List<String> inList = Arrays.asList("abc", "def");
            EmbulkTestOutputPlugin.clearResult();
            tester.runOutput(inList, parser, out);
            List<OutputRecord> result = EmbulkTestOutputPlugin.getResult();
            assertEquals(2, result.size());
            assertEquals("abc", result.get(0).getAsString(columnName));
            assertEquals("def", result.get(1).getAsString(columnName));
        }
    }

    @Test
    public void runFormatterToBinary1() throws IOException {
        runFormatterToBinary(1);
    }

    @Test
    public void runFormatterToBinary4() throws IOException {
        runFormatterToBinary(4);
    }

    private void runFormatterToBinary(int outputMinTaskSize) throws IOException {
        try (EmbulkPluginTester tester = new EmbulkPluginTester()) {
            tester.setOutputMinTaskSize(outputMinTaskSize);

            EmbulkTestParserConfig parser = tester.newParserConfig("csv");
            parser.addColumn("id", "long");
            parser.addColumn("text", "string");

            ConfigSource formatter = tester.newConfigSource("csv");
            formatter.set("header_line", false);
            formatter.set("delimiter", "\t");

            List<String> inList = Arrays.asList("11,abc", "22,def");
            List<byte[]> resultList = tester.runFormatterToBinary(inList, parser, formatter);
            assertEquals(outputMinTaskSize, resultList.size());
            int emptyCount = 0;
            for (byte[] result : resultList) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(result), StandardCharsets.UTF_8))) {
                    {
                        String line = reader.readLine();
                        if (line == null) {
                            emptyCount++;
                            continue;
                        }
                        assertEquals("11\tabc", line);
                    }
                    {
                        String line = reader.readLine();
                        assertEquals("22\tdef", line);
                    }
                    String line = reader.readLine();
                    assertNull(line);
                }
            }
            assertEquals(outputMinTaskSize - 1, emptyCount);
        }
    }
}
