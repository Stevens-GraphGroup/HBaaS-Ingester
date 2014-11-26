Ingest protein sequences into Accumulo.

Currently this has a Combiner-type iterator in the style of StatsCombiner called [ProteinStatsCombiner](src/main/java/edu/stevens/ProteinStatsCombiner.java). It counts the frequencies of each amino acid (and of gaps and degenerate characters and such).

[![Build Status](https://travis-ci.org/Stevens-GraphGroup/HBaaS-Ingester.svg)](https://travis-ci.org/Stevens-GraphGroup/HBaaS-Ingester)



`mvn package -DskipTests=true` to compile and build JARs.

`mvn test` to run tests.

