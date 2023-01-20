package pl.kielce.tu.psr.neo4j.embed;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

public class TestNeo4J {

	private static void createAll(GraphDatabaseService service) {
		Transaction tx = service.beginTx();
		try {
			createPatientCQL(tx, "Kowalski");
			createPatientCQL(tx, "Dobrowolski");
//			createGroupCQL(tx, "102");
//			createGroupCQL(tx, "103");
//			createRelationshipCQL(tx, "Polak", "102");
//			createRelationshipCQL(tx, "Nowak", "103");
			
//			Node kowalski = createStudent(tx, "Kowalski");
//			Node g101 = createGroup(tx, "101");
//			createRelationship(tx, kowalski, g101);
		} catch (Exception ex) {
			ex.printStackTrace();
			tx.rollback();
			return;
		}
		tx.commit();
	}
	
	private static void createPatientCQL(Transaction tx, String name) {
		String command = "CREATE (:Patient {nazwisko:$name})";
		System.out.println("Executing: " + command);
		Map<String, Object> parameters = new HashMap<>();
		parameters.put("name", name);
		tx.execute(command, parameters);
	}

	public static void createGroupCQL(Transaction tx, String groupName) {
		String command = "CREATE (:Grupa {nazwa:$groupName})";
		System.out.println("Executing: " + command);
		Map<String, Object> parameters = new HashMap<>();
		parameters.put("groupName", groupName);
		tx.execute(command, parameters);
	}

	public static void createRelationshipCQL(Transaction tx, String studentName, String groupName) {
		String command = "MATCH (s:Student),(g:Grupa) " + "WHERE s.nazwisko = $studentName AND g.nazwa = $groupName "
				+ "CREATE (s)-[r:JEST_W]->(g)" + "RETURN type(r)";
		Map<String, Object> parameters = new HashMap<>();
		parameters.put("studentName", studentName);
		parameters.put("groupName", groupName);
		System.out.println("Executing: " + command);
		tx.execute(command, parameters);
	}
	
	private static Node createStudent(Transaction tx, String studentName) {	
		System.out.println("Executing: Create Node Student");
		Node node = tx.createNode();
		node.addLabel(Label.label("Student"));
		node.setProperty("studentName", studentName);
		return node;
	}

	public static Node createGroup(Transaction tx, String groupName) {
		System.out.println("Executing: Create Node Grupa");
		Node node = tx.createNode();
		node.addLabel(Label.label("Grupa"));
		node.setProperty("nazwa", groupName);
		return node;
	}

	public static void createRelationship(Transaction tx, Node student, Node group) {
		student.createRelationshipTo(group, RelationshipType.withName("JEST_W"));
	}

	private static void readAll(GraphDatabaseService service) {
		Transaction tx = service.beginTx();
		try {
			readAllNodes(tx);
//			readAllRealtionships(tx);
//			readAllNodesWithRealationships(tx);
//			readAllNodesWithLabel(tx);
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

		Result result = tx.execute(command);
		printResult(result);
	}

	public static void readAllRealtionships(Transaction tx) {
		String command = "MATCH ()-[r]->() " + "RETURN r;";
		System.out.println("Executing: " + command);

		Result result = tx.execute(command);
		printResult(result);
	}

	public static void readAllNodesWithRealationships(Transaction tx) {
		String command = "MATCH (n1)-[r]-(n2) " + "RETURN n1, r, n2 ";
		System.out.println("Executing: " + command);

		Result result = tx.execute(command);
		printResult(result);
	}

	public static void readAllNodesWithLabel(Transaction tx) {
		String command = "MATCH (s:Student)-[r]-(n) " + "RETURN s, r, n";
		System.out.println("Executing: " + command);

		Result result = tx.execute(command);
		printResult(result);
	}

	private static void printResult(Result result) {
		while (result.hasNext()) {
			Map<String, Object> record = result.next();
			for (Entry<String, Object> field : record.entrySet())
				printEntity((Entity) field.getValue());
		}
	}

	public static void printEntity(Entity entity) {
		if (entity instanceof Node) {
			Node node = (Node) entity;
			printNode(node);
		} else if (entity instanceof Relationship) {
			Relationship relationship = (Relationship) entity;
			printRelationship(relationship);
		} else
			throw new RuntimeException();
	}

	public static void printNode(Node node) {
		System.out.println("node id = " + node.getId() + ", degree = " + node.getDegree() + ", labels = " + " : "
				+ node.getLabels() + ", properties = " + node.getAllProperties());
	}

	public static void printRelationship(Relationship relationship) {
		System.out.println("relationship id = " + relationship.getId() + ", type = " + relationship.getType()
				+ ", startNodeId = " + relationship.getStartNodeId() + ", endNodeId = " + relationship.getEndNodeId()
				+ ", properties = " + relationship.getAllProperties());
	}

	public static void deleteAll(GraphDatabaseService service) {
		String command = "MATCH (n) DETACH DELETE n";
		System.out.println("Executing: " + command);
		Transaction tx = service.beginTx();
		try {
			tx.execute(command);
		} catch (Exception ex) {
			ex.printStackTrace();
			tx.rollback();
			return;
		}
		tx.commit();
	}

	public static void deleteById(GraphDatabaseService service, Long id) {
		String command = String.format("MATCH (p:Patient) where ID(p)=%s DETACH DELETE p", id);
		System.out.println("Executing: " + command);
		Transaction tx = service.beginTx();
		try {
			tx.execute(command);
		} catch (Exception ex) {
			ex.printStackTrace();
			tx.rollback();
			return;
		}
		tx.commit();
	}

	public static void main(String[] args) throws Exception {

		Path databaseDirectory = Path.of("neo4jDir");

		DatabaseManagementService managementService = new DatabaseManagementServiceBuilder(databaseDirectory).build();
		Runtime.getRuntime().addShutdownHook(new Thread(() -> managementService.shutdown()));

		System.out.println(managementService.listDatabases());
		GraphDatabaseService service = managementService.database(DEFAULT_DATABASE_NAME);

		createAll(service);

		while (true)  {
			System.out.println("Wprowadź nr operacji: \n 1 - wyswietl liste pacjentów \n 2 - dodaj pacjenta \n 3 - skasuj pacjenta\n 0 - wyjście");
			Scanner s= new Scanner(System.in);
			char operationNumber = s.next().charAt(0);
			if (Character.getNumericValue(operationNumber) == 0) {
				break;
			}
			processUserInput(operationNumber, service);
		}

		deleteAll(service);
	}

	private static void processUserInput(int userInput, GraphDatabaseService service) {
		int numericValue = Character.getNumericValue(userInput);
		switch(numericValue) {
			case 1:
				getAllPatients(service);
				break;
			case 2:
				addPatient(service);
				break;
			case 3:
				deletePatient(service);
				break;
			default:
				System.out.println("Wybor niewlasciwy, sprobuj raz jeszcze.\n");
		}
	}

	private static void deletePatient(GraphDatabaseService service) {
		System.out.println("Którego pacjenta usunąć? Podaj jego id:\n");
		Scanner s= new Scanner(System.in);
		String key = s.nextLine();
		long id = Long.parseLong(key);
		deleteById(service, id);
	}

	private static void addPatient(GraphDatabaseService service) {
		System.out.println("Podaj nazwisko pacjenta:\n");
		Scanner s= new Scanner(System.in);
		String patientName = s.nextLine();
		Transaction tx = service.beginTx();
		try {
			createPatientCQL(tx, patientName);
		} catch (Exception ex) {
			ex.printStackTrace();
			tx.rollback();
			return;
		}
		tx.commit();
	}

	private static void getAllPatients(GraphDatabaseService service) {
		System.out.println("GET-ALL");
		readAll(service);
	}
}
