# hm-embulk-tester
Tool to test Embulk plugin

## required

* Embulk 0.10 or later
* Java8 or later

## usage

### ParserPlugin test

```java
import com.hishidama.embulk.tester.EmbulkPluginTester;
import com.hishidama.embulk.tester.EmbulkTestOutputPlugin.OutputRecord;
import com.hishidama.embulk.tester.EmbulkTestParserConfig;

@Test
public void test() {
    try (EmbulkPluginTester tester = new EmbulkPluginTester()) {
        // register test target plugin class
        tester.addParserPlugin("example1", Example1ParserPlugin.class);

        EmbulkTestParserConfig parser = tester.newParserConfig("example1"); // set test target plugin type
        parser.set("example1-option", "test");
        parser.addColumn("c1", "long");
        parser.addColumn("c2", "double");
        parser.addColumn("c3", "string");
        parser.addColumn("c4", "timestamp").set("format", "%Y/%m/%d");
        ... // set other option

        URL inFile = getClass().getResource("testFile.txt");
        List<OutputRecord> outputList = tester.runParser(inFile, parser);

        assertEquals(7, outputList.size());
        int i = 0;
        {
            OutputRecord record = outputList.get(i++);
            assertEquals(1L, record.getAsLong("c1"));
            assertEquals(2d, record.getAsDouble("c2"));
            assertEquals("3", record.getAsString("c3"));
            assertEquals(ZonedDateTime.parse("2023-06-19T10:15:30+09:00[Asia/Tokyo]").toInstant(), record.getAsTimestamp("c4"));
        }
        ...
    }
}
```

### OutputPlugin test

```java
import com.hishidama.embulk.tester.EmbulkPluginTester;

@Test
public void test() {
    // Java11
    try (var tester = new EmbulkPluginTester()) {
        // register test target plugin class
        tester.addOutputPlugin("example1", Example1OutputPlugin.class);

        var inputList = List.of( // csv - column c1,c2
            "abc,11",
            "def,22",
            "ghi,33");

        var parser = tester.newParserConfig("csv");
        parser.addColumn("c1", "string"); // column c1
        parser.addColumn("c2", "long");   // column c2

        var out = tester.newConfigSource();
        out.set("type", "example1"); // set test target plugin type
        ... // set other option

        tester.runOutput(inputList, parser, out);

        ... // assert output of Example1OutputPlugin
    }
}
```

## build for local

```bash
$ export JAVA_HOME=.../jdk1.8
$ cd .../hm-embulk-tester
$ ./gradlew publishToMavenLocal
```

### build.gradle(Gradle6) for test target

```gradle
repositories {
    mavenLocal() // ★
    mavenCentral()
}


dependencies {
    ...
    testCompile "org.embulk:embulk-junit4:0.10.41"
    testCompile "io.github.hishidama.embulk:hm-embulk-tester:0.1.+" // ★
    // https://mvnrepository.com/artifact/javax.xml.bind/jaxb-api
    testCompile 'javax.xml.bind:jaxb-api:2.3.1' // for Java11 Embulk v0.10
}
```

## import into Eclipse

1. `File` (menu bar) -> `Import`
2. `Gradle` -> `Existing Gradle Project`
