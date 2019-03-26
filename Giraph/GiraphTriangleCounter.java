package org.apache.giraph.examples;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * Calculates triangles using Giraph assuming triangle ABC
 */
public class GiraphTriangleCounter extends BasicComputation<
        IntWritable, IntWritable, NullWritable, IntWritable> {

    private static final Log LOG = LogFactory.getLog(GiraphTriangleCounter.class);

    @Override
    public void compute(
            Vertex<IntWritable, IntWritable, NullWritable> vertex,
            Iterable<IntWritable> recievedMessages) throws IOException {

    	LOG.info("Starting computation for vertex: " + vertex.getId().get());
    	
        long superstep = getSuperstep();
        IntWritable vertexId = vertex.getId();
        /**
         * All vertices send their IDs to their neighbors which have a greater ID
         * (messages travel on edges of type A B)
         */
        if (superstep == 0) {
        	sendEdgeABMessage(vertex.getEdges(), vertexId);
        }
        /**
         * All vertices forward the messages they received to their neighbors which have even a greater ID
         * (messages travel on edges of type B C)
         */
        if (superstep == 1) {
        	forwardBCMessage(vertex.getEdges(), recievedMessages, vertexId);
        }
        /**
         * All vertices forward the messages they received to all their neighbors
         * (in the hope of discovering C A edges)
         */
        if (superstep == 2) {
        	forwardCAMessage(vertex.getEdges(), recievedMessages);
        }
        /**
         * All vertices count how many messages received in the previous step contain their ID
         * (making them the vertices with the smallest ID in that triangle)
         */
        if (superstep == 3) {
        	countIncommingMessages(vertex, recievedMessages);
        }

        vertex.voteToHalt();

    }

	private void sendEdgeABMessage(Iterable<Edge<IntWritable, NullWritable>> edges, IntWritable vertexId) {
        for (Edge<IntWritable, NullWritable> edge : edges) {
        	IntWritable targetVertexId = edge.getTargetVertexId();
			if (targetVertexId.get() > vertexId.get()) {
                sendMessage(targetVertexId, vertexId);
            }
        }
	}

	private void forwardBCMessage(Iterable<Edge<IntWritable, NullWritable>> edges, Iterable<IntWritable> recievedMessages, IntWritable vertexId) {
        for (IntWritable message: recievedMessages) {
            for (Edge<IntWritable, NullWritable> edge : edges) {
                IntWritable targetVertexId = edge.getTargetVertexId();
				if (targetVertexId.get() > vertexId.get()) {
                    sendMessage(targetVertexId, message);
                }
            }
        }
	}

	private void forwardCAMessage(Iterable<Edge<IntWritable, NullWritable>> edges, Iterable<IntWritable> recievedMessages) {
        for (IntWritable message: recievedMessages) {
            for (Edge<IntWritable, NullWritable> edge : edges) {
                sendMessage(edge.getTargetVertexId(), message);
            }
        }
	}

	private void countIncommingMessages(Vertex<IntWritable, IntWritable, NullWritable> vertex, Iterable<IntWritable> recievedMessages) {
        int numberOfTriangles = 0;
        for (IntWritable message: recievedMessages) {
            if(message.get() == vertex.getId().get()){
                numberOfTriangles ++;
            }
        }
        vertex.setValue(new IntWritable(numberOfTriangles));
	}
}