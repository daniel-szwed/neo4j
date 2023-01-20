package pl.kielce.tu.psr.neo4j.simple;

import static org.neo4j.driver.internal.types.InternalTypeSystem.TYPE_SYSTEM;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.neo4j.driver.util.Pair;

public class TestNeo4J {

	private static void createAll(Session session) {
		Transaction tx = session.beginTransaction();
		try {
			createStudent(tx, "Polak");
			createStudent(tx, "Kowalski");
			createGroup(tx, "101");
			createGroup(tx, "102");
			createGroup(tx, "103");
			createRelationship(tx, "Kowalski", "101");
		} catch (Exception ex) {
			ex.printStackTrace();
			tx.rollback();
			return;
		}
		tx.commit();
	}

	private static void createStudent(Transaction tx, String studentName) {
		String command = "CREATE (:Student {nazwisko:$studentName})";
		System.out.println("Executing: " + command);
		Map<String, Object> parameters = new HashMap<>();
		parameters.put("studentName", studentName);
		tx.run(command, parameters);
	}

	public static void createGroup(Transaction tx, String groupName) {
		String command = "CREATE (:Grupa {nazwa:$groupName})";
		System.out.println("Executing: " + command);
		Map<String, Object> parameters = new HashMap<>();
		parameters.put("groupName", groupName);
		tx.run(command, parameters);
	}

	public static void createRelationship(Transaction tx, String studentName, String groupName) {
		String command = "MATCH (s:Student),(g:Grupa) " + "WHERE s.nazwisko = $studentName AND g.nazwa = $groupName "
				+ "CREATE (s)-[r:JEST_W]->(g)" + "RETURN type(r)";
		Map<String, Object> parameters = new HashMap<>();
		parameters.put("studentName", studentName);
		parameters.put("groupName", groupName);
		System.out.println("Executing: " + command);
		tx.run(command, parameters);
	}

	private static void readAll(Session session) {
		Transaction tx = session.beginTransaction();
		try {
			readAllNodes(tx);
			readAllRealtionships(tx);
			readAllNodesWithRealationships(tx);
			readAllNodesWithLabel(tx);
		} catch (Exception ex) {
			ex.printStackTrace();
			tx.rollback();
			return;
		}
		tx.commit();
	}

	private static void readAllNodes(Transaction tx) {
		String command = "MATCH (n) " + "RETURN n";
		System.out.println("Executing: " + command);

		Result result = tx.run(command);
		printResult(result);
	}

	public static void readAllRealtionships(Transaction tx) {
		String command = "MATCH ()-[r]->() " + "RETURN r;";
		System.out.println("Executing: " + command);

		Result result = tx.run(command);
		printResult(result);
	}

	public static void readAllNodesWithRealationships(Transaction tx) {
		String command = "MATCH (n1)-[r]-(n2) " + "RETURN n1, r, n2 ";
		System.out.println("Executing: " + command);

		Result result = tx.run(command);
		printResult(result);
	}

	public static void readAllNodesWithLabel(Transaction tx) {
		String command = "MATCH (s:Student)-[r]-(n) " + "RETURN s, r, n";
		System.out.println("Executing: " + command);

		Result result = tx.run(command);
		printResult(result);
	}

	private static void printResult(Result result) {
		while (result.hasNext()) {
			Record record = result.next();
			for (Pair<String, Value> field : record.fields())
				printValue(field.value());
		}
	}

	public static void printValue(Value value) {
		if (TYPE_SYSTEM.NODE().isTypeOf(value))
			printNode(value.asNode());
		else if (TYPE_SYSTEM.RELATIONSHIP().isTypeOf(value))
			printRelationship(value.asRelationship());
		else
			throw new RuntimeException();
	}

	public static void printNode(Node node) {
		System.out.println("node id = " + node.id() + ", size = " + node.size() + ", labels = " + " : " + node.labels()
				+ ", " + node.asMap());
	}

	public static void printRelationship(Relationship relationship) {
		System.out.println("relationship id = " + relationship.id() + ", type = " + relationship.type()
				+ ", startNodeId = " + relationship.startNodeId() + ", endNodeId = " + relationship.endNodeId() + ", "
				+ relationship.asMap());
	}

	public static void deleteAll(Session session) {
		String command = "MATCH (n) DETACH DELETE n";
		System.out.println("Executing: " + command);
		Transaction tx = session.beginTransaction();
		try {
			tx.run(command);
		} catch (Exception ex) {
			ex.printStackTrace();
			tx.rollback();
			return;
		}
		tx.commit();
	}

	public static void main(String[] args) throws Exception {
		try (Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "neo4jpassword"));
				Session session = driver.session()) {

			createAll(session);

			readAll(session);

			deleteAll(session);
		}
	}
}
