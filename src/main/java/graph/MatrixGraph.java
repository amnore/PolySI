package graph;

import java.util.ArrayDeque;
import java.util.Set;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.graph.ElementOrder;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.Graph;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;

import org.apache.commons.lang3.tuple.Pair;

import util.UnimplementedError;

public class MatrixGraph<T> implements Graph<T> {
	private final BiMap<T, Integer> nodeMap = HashBiMap.create();
	private final byte adjacency[][];

	public MatrixGraph(Graph<T> graph, boolean isRWEdge) {
		int i = 0;
		for (var n : graph.nodes()) {
			nodeMap.put(n, i++);
		}

		adjacency = new byte[i][i];
		for (var e : graph.edges()) {
			adjacency[nodeMap.get(e.source())][nodeMap.get(e.target())] = (byte) (1 | (isRWEdge ? 0 : 2));
		}
	}

	private MatrixGraph(BiMap<T, Integer> nodes) {
		nodeMap.putAll(nodes);
		adjacency = new byte[nodes.size()][nodes.size()];
	}

	private MatrixGraph(MatrixGraph<T> graph) {
		nodeMap.putAll(graph.nodeMap);
		adjacency = new byte[graph.adjacency.length][];
		for (var i = 0; i < adjacency.length; i++) {
			adjacency[i] = graph.adjacency[i].clone();
		}
	}

	@Override
	public Set<T> nodes() {
		return nodeMap.keySet();
	}

	@Override
	public Set<EndpointPair<T>> edges() {
		throw new UnimplementedError();
	}

	@Override
	public boolean isDirected() {
		return true;
	}

	@Override
	public boolean allowsSelfLoops() {
		return true;
	}

	@Override
	public ElementOrder<T> nodeOrder() {
		return ElementOrder.unordered();
	}

	@Override
	public ElementOrder<T> incidentEdgeOrder() {
		return ElementOrder.unordered();
	}

	@Override
	public Set<T> adjacentNodes(T node) {
		throw new UnimplementedError();
	}

	@Override
	public Set<T> predecessors(T node) {
		throw new UnimplementedError();
	}

	@Override
	public Set<T> successors(T node) {
		throw new UnimplementedError();
	}

	@Override
	public Set<EndpointPair<T>> incidentEdges(T node) {
		throw new UnimplementedError();
	}

	@Override
	public int degree(T node) {
		throw new UnimplementedError();
	}

	@Override
	public int inDegree(T node) {
		throw new UnimplementedError();
	}

	@Override
	public int outDegree(T node) {
		throw new UnimplementedError();
	}

	@Override
	public boolean hasEdgeConnecting(T nodeU, T nodeV) {
		return (adjacency[nodeMap.get(nodeU)][nodeMap.get(nodeV)] & 1) != 0;
	}

	@Override
	public boolean hasEdgeConnecting(EndpointPair<T> endpoints) {
		return hasEdgeConnecting(endpoints.source(), endpoints.target());
	}

	public boolean hasNonRWEdgeConnecting(T nodeU, T nodeV) {
		return adjacency[nodeMap.get(nodeU)][nodeMap.get(nodeV)] == 3;
	}

	private static int concatEdge(int edge1, int edge2) {
		var has_edge = edge1 & edge2 & 1;
		var has_non_rw_edge = (edge2 & 2 & (has_edge << 1));
		return has_edge | has_non_rw_edge;
	}

	private MatrixGraph<T> floyd() {
		var result = new MatrixGraph<>(this);
		for (var i = 0; i < adjacency.length; i++) {
			result.adjacency[i][i] = 3;
		}

		for (var k = 0; k < adjacency.length; k++) {
			for (var i = 0; i < adjacency.length; i++) {
				for (var j = 0; j < adjacency.length; j++) {
					result.adjacency[i][j] |= concatEdge(result.adjacency[i][k], result.adjacency[k][j]);
				}
			}
		}

		return result;
	}

	private MatrixGraph<T> allNodesBfs() {
		var result = new MatrixGraph<>(this.nodeMap);
		var graph = toSparseGraph();
		for (var i = 0; i < adjacency.length; i++) {
			var q = new ArrayDeque<Integer>();
			var reachable = result.adjacency[i];

			q.add(i);
			reachable[i] = 3;
			while (!q.isEmpty()) {
				var j = q.pop();

				for (var k : graph.successors(j)) {
					var isNew = (reachable[k] & 1) == 0;
					reachable[k] |= concatEdge(1, adjacency[j][k]);

					if (isNew) {
						q.push(k);
					}
				}
			}
		}

		return result;
	}

	public MatrixGraph<T> reachability() {
		var result = allNodesBfs();

		System.err.println();
		for (int i = 0; i < adjacency.length; i++) {
			for (int j = 0; j < adjacency.length; j++) {
				System.err.printf("%d ", result.adjacency[i][j] & 1);
			}
			System.err.println();
		}

		return result;
	}

	private MatrixGraph<T> matrixProduct(MatrixGraph<T> other) {
		assert nodeMap.equals(other.nodeMap);

		var result = new MatrixGraph<>(nodeMap);
		for (var i = 0; i < adjacency.length; i++) {
			for (var j = 0; j < adjacency.length; j++) {
				for (var k = 0; k < adjacency.length; k++) {
					result.adjacency[i][k] |= concatEdge(adjacency[i][j], other.adjacency[j][k]);
				}
			}
		}

		return result;
	}

	private MatrixGraph<T> sparseComposition(MatrixGraph<T> other) {
		assert nodeMap.equals(other.nodeMap);

		var result = new MatrixGraph<>(nodeMap);
		var a = toSparseGraph();
		var b = other.toSparseGraph();
		for (var i = 0; i < adjacency.length; i++) {
			for (var j : a.predecessors(i)) {
				for (var k : b.successors(i)) {
					result.adjacency[j][k] |= concatEdge(adjacency[j][i], other.adjacency[i][k]);
				}
			}
		}

		return result;
	}

	public MatrixGraph<T> composition(MatrixGraph<T> other) {
		return sparseComposition(other);
	}

	public MatrixGraph<T> union(MatrixGraph<T> other) {
		assert nodeMap.equals(other.nodeMap);

		var result = new MatrixGraph<>(nodeMap);
		for (var i = 0; i < adjacency.length; i++) {
			for (var j = 0; j < adjacency.length; j++) {
				result.adjacency[i][j] = (byte) (adjacency[i][j] | other.adjacency[i][j]);
			}
		}

		return result;
	}

	private Graph<Integer> toSparseGraph() {
		MutableGraph<Integer> graph = GraphBuilder.directed().allowsSelfLoops(true).build();
		for (int i = 0; i < adjacency.length; i++) {
			graph.addNode(i);
		}
		for (int i = 0; i < adjacency.length; i++) {
			for (int j = 0; j < adjacency.length; j++) {
				if ((adjacency[i][j] & 1) != 0) {
					graph.putEdge(i, j);
				}
			}
		}

		return graph;
	}
}
