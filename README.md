adam-queries
============

[![Build Status](https://travis-ci.org/fnothaft/adam-queries.svg?branch=master)](https://travis-ci.org/fnothaft/adam-queries)

Small queries to run on ADAM for GDA.

Both queries take two arguments; these are the two input datasets to run on. For this example,
let's use [NA12878](ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/data/NA12878/alignment/NA12878.mapped.ILLUMINA.bwa.CEU.low_coverage.20121211.bam)
and [HG00096](ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/data/HG00096/alignment/HG00096.mapped.ILLUMINA.bwa.GBR.low_coverage.20120522.bam).

To compile, run:

```
mvn package
```

This will create two JARs which can be run via Spark Submit.

Query 1
=======

Counts the _k_-mers (substrings of length _k_) in the reads of two datasets. Then,
we measure the number of unique strings, as well as the total number of strings
in each dataset, as well as the number of _k_-mers and the count of _k_-mer occurrences
that are common to _both_ datasets.

In this query, _k_ is set to 20.

Query 2
=======

Counts the tag strings annotated to the reads of two datasets. Then,
we measure the number of unique strings, as well as the total number of strings
in each dataset, as well as the number of tag strings and the count of tag string occurrences
that are common to _both_ datasets.

Query 3
=======

Calculates the concordance of two sets of genotypes.

Query 4
=======

Trains a _k_-means model on the _k_-mers in a set of reads with the _k_ for _k_-mers
set to 20 and the number of cluster centroids set to 2.

Query 5
=======

Trains a linear regression model on the GC bias distribution of a set of _k_-mers,
using the dinucleotide composition and _k_-mer multiplicites.

Query 6
=======

Applies a Yates correction to reads to correct the base quality scores of the reads,
using a table of SNP evidence.
