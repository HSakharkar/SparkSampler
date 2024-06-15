# Spark Sampler

This project help us to extract samples from the big dataset i.e RDD. Dataset and DataFrame in an efficient way.

## Building Spark Sampler
Spark Sampler used sbt to build

Compile
```bash
./build/sbt compile
```

Package
```bash
./build/sbt package
```

## Usage

Import the Converter
```
import org.open.spark.sampler.SamplerConverter._
```

Extract Samples from Dataframe
```
 val sampleDF = PersonDF.sampleBy("columnname", 50) // extract 50 samples

 val mapSampleDF = PersonDF.sampleBy("columnname", Map("MS"->0.1, "FM"->0.2, "FX"->0.3, "ML"->0.4), 100000) // extract 100000 of specific type of dataset based on key with provided ratio.
```