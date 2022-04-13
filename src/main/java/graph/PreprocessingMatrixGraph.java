package graph;

import java.util.ArrayDeque;
import java.util.Set;
import java.util.Arrays;
import java.util.HashSet;

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
    private final long adjacency[][];

    public PreprocessingMatrixGraph(Graph<T> graph, boolean isRWEdge) {
        int i = 0;
        for (var n : graph.nodes()) {
            nodeMap.put(n, i++);
        }

        adjacency = new long[i][(i * 2 + Long.BYTES) / Long.BYTES];
        for (var e : graph.edges()) {
            set(e.source(), e.target(), isRWEdge);
        }
    }

    private PreprocessingMatrixGraph(BiMap<T, Integer> nodes) {
        nodeMap.putAll(nodes);
        adjacency = new long[nodes.size()][(nodes.size() * 2 + Long.BYTES) / Long.BYTES];
    }

    private PreprocessingMatrixGraph(PreprocessingMatrixGraph<T> graph) {
        nodeMap.putAll(graph.nodeMap);
        adjacency = new long[graph.adjacency.length][];
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
        var result = new HashSet<EndpointPair<T>>();
        var map = nodeMap.inverse();

        for (int i = 0; i < adjacency.length; i++) {
            for (int j = 0; j < adjacency.length; j++) {
                if (get(i, j) != 0) {
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
        return get(nodeMap.get(nodeU), nodeMap.get(nodeV)) != 0;
    }

    @Override
    public boolean hasEdgeConnecting(EndpointPair<T> endpoints) {
        return hasEdgeConnecting(endpoints.source(), endpoints.target());
    }

    public boolean hasNonRWEdgeConnecting(T nodeU, T nodeV) {
        return get(nodeMap.get(nodeU), nodeMap.get(nodeV)) == 3;
    }

    private static long concatEdge(int inEdge, long outEdges) {
        return inEdge == 0 ? 0 : outEdges;
    }

    private PreprocessingMatrixGraph<T> floyd() {
        var result = new PreprocessingMatrixGraph<>(this);
        for (var i = 0; i < adjacency.length; i++) {
            result.set(i, i, false);
        }

        for (var k = 0; k < adjacency.length; k++) {
            for (var i = 0; i < adjacency.length; i++) {
                var e = result.get(i, k);
                if (e == 0) {
                    continue;
                }

                for (var j = 0; j < adjacency[0].length; j++) {
                    result.adjacency[i][j] |= concatEdge(e, result.adjacency[k][j]);
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

            q.add(i);
            result.set(i, i, false);
            while (!q.isEmpty()) {
                var j = q.pop();

                for (var k : graph.successors(j)) {
                    var isNew = result.get(i, k) == 0;
                    result.set(i, k, get(j, k) == 3);

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
                var e = get(i, j);
                if (e == 0) {
                    continue;
                }

                for (var k = 0; k < adjacency[0].length; k++) {
                    result.adjacency[i][k] |= concatEdge(e, other.adjacency[j][k]);
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
                    result.set(j, k, get(i, k) == 3);
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
                if (get(i, j) != 0) {
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
                builder.append(get(i, j));
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

    private void set(int i, int j, boolean isRW) {
        var arr = adjacency[i];
        var pos = j * 2 / Long.BYTES;
        var bitpos = j * 2 % Long.BYTES;

        arr[pos] |= (1L | (isRW ? 2L : 0L)) << bitpos;
    }

    private void set(T nodeU, T nodeV, boolean isRW) {
        set(nodeMap.get(nodeU), nodeMap.get(nodeV), isRW);
    }

    private int get(int i, int j) {
        return (int) (adjacency[i][j * 2 / Long.BYTES] >> (j * 2 % Long.BYTES)) & 3;
    }
}
