package graph;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Streams;
import com.google.common.graph.ElementOrder;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.Graph;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;

import util.UnimplementedError;

public class MatrixGraph<T> implements Graph<T> {
    private final BiMap<T, Integer> nodeMap = HashBiMap.create();
    private final long adjacency[][];

    public MatrixGraph(Graph<T> graph) {
        int i = 0;
        for (var n : graph.nodes()) {
            nodeMap.put(n, i++);
        }

        adjacency = new long[i][(i + Long.BYTES - 1) / Long.BYTES];
        for (var e : graph.edges()) {
            set(e.source(), e.target());
        }
    }

    private MatrixGraph(BiMap<T, Integer> nodes) {
        nodeMap.putAll(nodes);
        adjacency = new long[nodes.size()][(nodes.size() + Long.BYTES - 1) / Long.BYTES];
    }

    private MatrixGraph(MatrixGraph<T> graph) {
        nodeMap.putAll(graph.nodeMap);
        adjacency = new long[graph.adjacency.length][];
        for (var i = 0; i < adjacency.length; i++) {
            adjacency[i] = graph.adjacency[i].clone();
        }
    }

    private MatrixGraph<T> floyd() {
        var result = new MatrixGraph<>(this);
        for (var i = 0; i < adjacency.length; i++) {
            result.set(i, i);
        }

        for (var k = 0; k < adjacency.length; k++) {
            for (var i = 0; i < adjacency.length; i++) {
                if (!result.get(i, k)) {
                    continue;
                }

                for (var j = 0; j < adjacency[0].length; j++) {
                    result.adjacency[i][j] |= result.adjacency[k][j];
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

            q.add(i);
            result.set(i, i);
            while (!q.isEmpty()) {
                var j = q.pop();

                for (var k : graph.successors(j)) {
                    if (result.get(i, k)) {
                        continue;
                    }

                    result.set(i, k);
                    q.push(k);
                }
            }
        }

        return result;
    }

    public MatrixGraph<T> reachability(String type) {
        switch (type) {
            case "sparse":
                return allNodesBfs();
            case "dense":
                return floyd();
            default:
                throw new RuntimeException("unknown type %s".formatted(type));
        }
    }

    private MatrixGraph<T> matrixProduct(MatrixGraph<T> other) {
        assert nodeMap.equals(other.nodeMap);

        var result = new MatrixGraph<>(nodeMap);
        for (var i = 0; i < adjacency.length; i++) {
            for (var j = 0; j < adjacency.length; j++) {
                if (!get(i, j)) {
                    continue;
                }

                for (var k = 0; k < adjacency[0].length; k++) {
                    result.adjacency[i][k] |= other.adjacency[j][k];
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
                    result.set(j, k);
                }
            }
        }

        return result;
    }

    public MatrixGraph<T> composition(String type, MatrixGraph<T> other) {
        switch (type) {
            case "sparse":
                return sparseComposition(other);
            case "dense":
                return matrixProduct(other);
            default:
                throw new RuntimeException("invalid type %s".formatted(type));
        }
    }

    public MatrixGraph<T> union(MatrixGraph<T> other) {
        assert nodeMap.equals(other.nodeMap);

        var result = new MatrixGraph<>(nodeMap);
        for (var i = 0; i < adjacency.length; i++) {
            for (var j = 0; j < adjacency[0].length; j++) {
                result.adjacency[i][j] = adjacency[i][j] | other.adjacency[i][j];
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
                if (get(i, j)) {
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
                builder.append(get(i, j) ? 1 : 0);
                builder.append(' ');
            }
            builder.append('\n');
        }

        return builder.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MatrixGraph)) {
            return false;
        }

        var g = (MatrixGraph<T>) obj;
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

    @Override
    public Set<T> nodes() {
        return nodeMap.keySet();
    }

    @Override
    public Set<EndpointPair<T>> edges() {
        var result = new HashSet<EndpointPair<T>>();
        var map = nodeMap.inverse();

        for (int i = 0; i < adjacency.length; i++) {
            for (int j = 0; j < adjacency.length; j++) {
                if (get(i, j)) {
                    result.add(EndpointPair.ordered(map.get(i), map.get(j)));
                }
            }
        }

        return result;
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
        return get(nodeMap.get(nodeU), nodeMap.get(nodeV));
    }

    @Override
    public boolean hasEdgeConnecting(EndpointPair<T> endpoints) {
        return hasEdgeConnecting(endpoints.source(), endpoints.target());
    }

    private boolean get(int i, int j) {
        return (adjacency[i][j / Long.BYTES] & (1L << (j % Long.BYTES))) != 0;
    }

    private void set(int i, int j) {
        adjacency[i][j / Long.BYTES] |= (1L << (j % Long.BYTES));
    }

    private void set(T nodeU, T nodeV) {
        set(nodeMap.get(nodeU), nodeMap.get(nodeV));
    }
}
