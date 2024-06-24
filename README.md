# Spark Sampler

This project help us to extract samples from the big dataset i.e RDD. Dataset and DataFrame in an efficient way.

This library provides a function for representative sampling to achieve data coverage. This representative sample can then be used for AI/ML training and test data creation.

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
## Compatibility version
```bash
Scala Version: 2.12
```

## Usage

Import the Converter
```
import org.open.spark.sampler.SamplerConverter._
```

Extract Samples from Dataframe
```
 val sampleDF = PersonDF.sampleBy("columnname", 50) // extract 50 samples with representative data sampling

 val mapSampleDF = PersonDF.sampleBy("columnname", Map("MS"->0.1, "FM"->0.2, "FX"->0.3, "ML"->0.4), 100000) // extract 100000 of specific type of dataset based on key with provided ratio.
```

## How it works 
https://medium.com/@hemantsakharkar/optimized-representative-data-sampling-for-ai-with-data-coverage-leveraging-apache-sparks-98b7aa53c7f7