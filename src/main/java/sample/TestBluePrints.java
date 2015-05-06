package sample;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import util.TriangleCount;
import util.WeakTiesCount;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;

public class TestBluePrints {

	public static void main(String[] args) {
		Graph graph = new TinkerGraph();
		Vertex a = graph.addVertex(null);
		Vertex b = graph.addVertex(null);
		Vertex c = graph.addVertex(null);
		Vertex d = graph.addVertex(null);
		a.setProperty("name", "marko");
		b.setProperty("name", "peter");
		c.setProperty("name", "kim");
		d.setProperty("name", "doo");
		Edge e = graph.addEdge(null, a, b, "knows");
		Edge e2 = graph.addEdge(null, b, c, "knows");
		Edge e3 = graph.addEdge(null, c, a, "knows3");
		Edge e4 = graph.addEdge(null, b, d, "knows4");
		Edge e5 = graph.addEdge(null, d, c, "k4");
		System.out.println(e.getVertex(Direction.OUT).getProperty("name") + "--" + e.getLabel() + "-->" + e.getVertex(Direction.IN).getProperty("name"));

		
		List<Edge> edges = new ArrayList<Edge>();
		edges.add(e);
//		edges.add(e2);
		edges.add(e3);
		edges.add(e4);
		edges.add(e5);
//		TriangleCount.counts(edges);
		WeakTiesCount.counts(edges);
		
//		TriangleCount.counts(Arrays.asList(new String[] { "ABC", "CDEF", "AIF", "BGHI", "CKJI", "AGKD", "AHJE" }));
	}

}
