# RADAR Schemas Java SDK

The Java SDKs are published as JARs on Maven Central. To use them in Gradle, add the following code to your `build.gradle`:

```gradle
repositories {
    mavenCentral()
    maven { url 'https://packages.confluent.io/maven/' }
    maven { url "https://jitpack.io" }
}

dependencies {
    // Commons schemas (backend, passive remote monitoring app)
    compile 'org.radarcns:radar-schemas-commons:<release version>'

    // Questionnaire schemas (active remote monitoring app)
    compile 'org.radarcns:radar-schemas-tools:<release version>'
}
```
Usually, you only need to include the schemas you actually need in your dependencies.

The generated code each refers to a single schema. The classes of Avro records will extend `org.apache.avro.specific.SpecificRecord`. They each have a static `getClassSchema()` function that returns the `Schema` that it was generated from. To read JSON serialized data for example, use the following code:

```java
public class Deserialize {
   public PhoneBatteryLevel deserializeBatteryLevel(InputStream json) throws Exception {
      //Instantiating the Schema.Parser class.
      DatumReader<PhoneBatteryLevel> datumReader = new SpecificDatumReader<>(PhoneBatteryLevel.class);
      Decoder decoder = new DecoderFactory().jsonDecoder(PhoneBatteryLevel.getClassSchema(), outputStream);
      return datumReader.read(null, decoder);
   }
}
```

Alternatively, use `org.radarcns.data.SpecificRecordEncoder` and `org.radarcns.data.SpecificRecordDecoder` from the [`radar-commons`](https://github.com/RADAR-base/radar-commons) package.
