package org.apache.spark.examples.datalog

import java.nio.file.{Files, Paths}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import edu.ucla.cs.wis.bigdatalog.spark.BigDatalogContext

import scala.collection.mutable.StringBuilder

  def runBigDatalogSSSP(bigDatalogCtx: BigDatalogContext, filePath: String, options: Map[String, String], startVertexId: Int): RDD[Row] = {
    val database = "database({arc(From: integer, To: integer, Cost: integer)})."
    /**
    The SSSP program computes the length of the shortest
    path from a source vertex to all vertices it is connected to.
    This program uses a mmin monotonic aggregate. Here the
    arc predicate in r3 denotes edges of the graph (C,B) with
    edge cost D2. r2 seeds the recursion with starting vertexId. 
    Then, r3 will recursively produce all new minimum cost
    paths to a node B though node C. R3's relation will contain the
    shortest path from starting vertexId to each vertex.
    */
    val rules = "leftLinearSP(B,min<C>) <- mminleftLinearSP(B,C)." +
      "mminleftLinearSP(B,mmin<C>) <- B=" + startVertexId + ", C=0." +
      "mminleftLinearSP(B,mmin<D>) <- mminleftLinearSP(C,D1), arc(C,B,D2), D=D1+D2."

    runBigDatalogProgram(bigDatalogCtx, database, rules, "leftLinearSP(A,B).", Seq(("arc", filePath)))
  }