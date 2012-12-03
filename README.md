morph-streams
=============

To build morph-streams you need:

* jvm7
* sbt (www.scala-sbt.org)

To compile it, run sbt after downloading the code:

```
>sbt
>compile
```

To see some sparql-stream queries running with esper, you can check some simple tests (QueryExecutionTest):

```
>sbt
>project adapter-esper
>test
```

