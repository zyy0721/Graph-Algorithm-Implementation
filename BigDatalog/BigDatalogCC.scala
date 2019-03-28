package org.apache.spark.examples.datalog

import java.nio.file.{Files, Paths}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import edu.ucla.cs.wis.bigdatalog.spark.BigDatalogContext

import scala.collection.mutable.StringBuilder

  def runBigDatalogCC(bigDatalogCtx: BigDatalogContext, filePath: String, options: Map[String, String]): RDD[Row] = {
    val database = "database({arc(From: integer, To: integer)})."
    /**
    This program works by initially assigning the node's id to itself (r1), 
    and then propagating a new lower node id for any edge the node is connected to. 
    r3 is necessary to select only the minimum node id Y for each X found in cc2.
    */
    val rules = "cc3(X,mmin<X>) <- arc(X,_)." +
      "cc3(Y,mmin<V>) <- cc3(X,V), arc(X,Y)." +
      "cc2(X,min<Y>) <- cc3(X,Y)." +
      "cc(countd<X>) <- cc2(_,X)."

    runBigDatalogProgram(bigDatalogCtx, database, rules, "cc(A).", Seq(("arc", filePath)))
  }