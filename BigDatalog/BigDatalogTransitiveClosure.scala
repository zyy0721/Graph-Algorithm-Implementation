package org.apache.spark.examples.datalog

import java.nio.file.{Files, Paths}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import edu.ucla.cs.wis.bigdatalog.spark.BigDatalogContext

import scala.collection.mutable.StringBuilder
  //In this non-recursive program, rule1 performs self-joins of arc to produce triangle occurrences which are then counted by rule2.
  def runBigDatalogTriangleCount(bigDatalogCtx: BigDatalogContext, filePath: String, options: Map[String, String]): Long = {
    val database = "database({arc(From: integer, To: integer)})."

    val rules = "tc(A,B) <- arc(A,B)."+"tc(A,B) <- tc(A,C), tc(C,B)."
       
   runBigDatalogProgram(bigDatalogCtx, database, rules, "tc(A,B).", Seq(("arc", filePath)))
   }