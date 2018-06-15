

  Local mode
 -Dspark.input.path=/tmp/spark/WordCountSpark/cfa1cf07-0e9e-4d05-9eca-ee1b571114c8/INPUT/
 -Dspark.output.path=/tmp/spark/WordCountSpark/cfa1cf07-0e9e-4d05-9eca-ee1b571114c8/OUTPUT
 -Dspark.is.run.local=true
 -Dspark.default.fs=file:///
 -Dspark.delimeter.value=,
 -Dspark.num.partitions=1


 to run on cluster


 -Dspark.input.path=/tmp/spark/WordCountSpark/cfa1cf07-0e9e-4d05-9eca-ee1b571114c8/INPUT/
 -Dspark.output.path=/tmp/spark/WordCountSpark/cfa1cf07-0e9e-4d05-9eca-ee1b571114c8/OUTPUT
 -Dspark.delimeter.value=,
 -Dspark.num.partitions=100 (depending on number of files in output )