# hm-embulk-tester
Tool to test Embulk plugin

## required

* Embulk 0.10 or later
* Java8 or later

## usage

### OutputPlugin test

```java
import com.hishidama.embulk.tester.EmbulkPluginTester;

@Test
public void test() {
    // Java11
    try (var tester = new EmbulkPluginTester()) {
        // register target plugin class
        tester.addOutputPlugin("example1", Example1OutputPlugin.class);

        var inputList = List.of( // csv - column c1,c2
            "abc,11",
            "def,22",
            "ghi,33");

        var parser = tester.newParserConfig("csv");
        parser.addColumn("c1", "string"); // column c1
        parser.addColumn("c2", "long");   // column c2

        var out = tester.newConfigSource();
        out.set("type", "example1"); // set target plugin type
        ... // set other option

        tester.runOutput(inputList, parser, out);

        ... // assert
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
    testCompile 'javax.xml.bind:jaxb-api:2.3.1' // for Java11
}
```

## import into Eclipse

1. `File` (menu bar) -> `Import`
2. `Gradle` -> `Existing Gradle Project`
