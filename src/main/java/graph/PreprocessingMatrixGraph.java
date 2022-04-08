package graph;

import java.util.ArrayDeque;
import java.util.Set;
import java.util.Arrays;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.graph.ElementOrder;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.Graph;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;

import org.apache.commons.lang3.tuple.Pair;

import util.UnimplementedError;

public class PreprocessingMatrixGraph<T> implements Graph<T> {
    private final BiMap<T, Integer> nodeMap = HashBiMap.create();
    private final byte adjacency[][];

    public PreprocessingMatrixGraph(Graph<T> graph, boolean isRWEdge) {
        int i = 0;
        for (var n : graph.nodes()) {
            nodeMap.put(n, i++);
        }

        adjacency = new byte[i][i];
        for (var e : graph.edges()) {
            adjacency[nodeMap.get(e.source())][nodeMap.get(e.target())] = (byte) (1 | (isRWEdge ? 0 : 2));
        }
    }

    private PreprocessingMatrixGraph(BiMap<T, Integer> nodes) {
        nodeMap.putAll(nodes);
        adjacency = new byte[nodes.size()][nodes.size()];
    }

    private PreprocessingMatrixGraph(PreprocessingMatrixGraph<T> graph) {
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
        var has_non_rw_edge = (edge2 & 2 & (edge1 << 1));
        return has_edge | has_non_rw_edge;
    }

    private PreprocessingMatrixGraph<T> floyd() {
        var result = new PreprocessingMatrixGraph<>(this);
        for (var i = 0; i < adjacency.length; i++) {
            result.adjacency[i][i] |= 1;
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

    private PreprocessingMatrixGraph<T> allNodesBfs() {
        var result = new PreprocessingMatrixGraph<>(this.nodeMap);
        var graph = toSparseGraph();
        for (var i = 0; i < adjacency.length; i++) {
            var q = new ArrayDeque<Integer>();
            var reachable = result.adjacency[i];

            q.add(i);
            reachable[i] = 1;
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

    public PreprocessingMatrixGraph<T> reachability(String type) {
        switch (type) {
            case "sparse":
                return allNodesBfs();
            case "dense":
                return floyd();
            default:
                throw new RuntimeException("unknown type %s".formatted(type));
        }
    }

    private PreprocessingMatrixGraph<T> matrixProduct(PreprocessingMatrixGraph<T> other) {
        assert nodeMap.equals(other.nodeMap);

        var result = new PreprocessingMatrixGraph<>(nodeMap);
        for (var i = 0; i < adjacency.length; i++) {
            for (var j = 0; j < adjacency.length; j++) {
                for (var k = 0; k < adjacency.length; k++) {
                    result.adjacency[i][k] |= concatEdge(adjacency[i][j], other.adjacency[j][k]);
                }
            }
        }

        return result;
    }

    private PreprocessingMatrixGraph<T> sparseComposition(PreprocessingMatrixGraph<T> other) {
        assert nodeMap.equals(other.nodeMap);

        var result = new PreprocessingMatrixGraph<>(nodeMap);
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

    public PreprocessingMatrixGraph<T> composition(String type, PreprocessingMatrixGraph<T> other) {
        switch (type) {
            case "sparse":
                return sparseComposition(other);
            case "dense":
                return matrixProduct(other);
            default:
                throw new RuntimeException("invalid type %s".formatted(type));
        }
    }

    public PreprocessingMatrixGraph<T> union(PreprocessingMatrixGraph<T> other) {
        assert nodeMap.equals(other.nodeMap);

        var result = new PreprocessingMatrixGraph<>(nodeMap);
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

    @Override
    public String toString() {
        var builder = new StringBuilder();

        builder.append('\n');
        for (int i = 0; i < adjacency.length; i++) {
            for (int j = 0; j < adjacency.length; j++) {
                builder.append(adjacency[i][j]);
                builder.append(' ');
            }
            builder.append('\n');
        }

        return builder.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PreprocessingMatrixGraph)) {
            return false;
        }

        var g = (PreprocessingMatrixGraph<T>) obj;
        if (!nodeMap.equals(g.nodeMap)) {
            return false;
        }

        for (var i = 0; i < adjacency.length; i++) {
            if (!Arrays.equals(adjacency[i], g.adjacency[i])) {
                return false;
            }
        }

        return true;
    }
}
