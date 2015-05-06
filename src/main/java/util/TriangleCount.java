package util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;

/**
 * This code is referenced 
 * http://www.careercup.com/question?id=5988741646647296
 * @author dw
 *
 */

public class TriangleCount {

	public static Map<Object,Set<Object>> buildAdjacencyMap(List<Edge> edges){
		if ((edges==null) || (edges.isEmpty())){
			return Collections.<Object,Set<Object>>emptyMap();
		}
		
		Map<Object,Set<Object>> graph = new HashMap<>();
		for (Edge e : edges){
			if (!graph.containsKey(e.getVertex(Direction.IN))){
				graph.put(e.getVertex(Direction.IN), new HashSet<Object>());
			}
			if (!graph.containsKey(e.getVertex(Direction.OUT))){
				graph.put(e.getVertex(Direction.OUT), new HashSet<Object>());
			}
			graph.get(e.getVertex(Direction.IN)).add(e.getVertex(Direction.OUT));
			graph.get(e.getVertex(Direction.OUT)).add(e.getVertex(Direction.IN));
		}
		
		return graph;
	}
	
	public static int counts(List<Edge> edges){

		Map<Object,Set<Object>> graph = buildAdjacencyMap(edges);

		int triangles = 0;
		for (Set<Object> neighbors : graph.values()){
			for (Object v2 : neighbors){
				for (Object v3 : neighbors){
					if ((!v2.equals(v3)) && (graph.get(v2).contains(v3))){
						triangles++;
					}
				}
			}
		}
		System.out.println(triangles/6);
		return (triangles/6);
	}
}
