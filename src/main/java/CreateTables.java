import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class CreateTables implements PersonDAO{

	private Connection connection;

	public CreateTables(Connection connection) throws ClassNotFoundException, SQLException {
		this.connection = connection;
	}

	public void addAllTables() throws SQLException {

		addProcessTable();
		addFileTable();
		addSocketTable();
		addRegistryTable();
		addModuleTable();
		addProcessEntitiesTable();
	}

	public void addProcessTable() throws SQLException {
		Statement stmt = this.connection.createStatement();
		String drop_sql = "DROP TABLE IF EXISTS process_events";
		stmt.executeUpdate(drop_sql);

//		String sql = "CREATE TABLE process_events(id varchar(50) not null primary key, timestamp varchar(50), hostname varchar(50), " +
//				"action varchar(10), actorID varchar(50), objectID varchar(50), ppid int, pid int, " +
//				"image_path varchar, parent_image_path varchar, command_line varchar)";

		String sql = "CREATE TABLE process_events(id varchar(40) not null primary key, timestamp timestamp, hostname varchar(50), " +
				"action varchar(10), actorID varchar(40), objectID varchar(40), command_line varchar)";

		stmt.executeUpdate(sql);
		stmt.close();
	}

	public void addProcessEntitiesTable() throws SQLException {
		Statement stmt = this.connection.createStatement();
		String drop_sql = "DROP TABLE IF EXISTS process_entities";
		stmt.executeUpdate(drop_sql);

//		String sql = "CREATE TABLE process_events(id varchar(50) not null primary key, timestamp varchar(50), hostname varchar(50), " +
//				"action varchar(10), actorID varchar(50), objectID varchar(50), ppid int, pid int, " +
//				"image_path varchar, parent_image_path varchar, command_line varchar)";

		String sql = "CREATE TABLE process_entities(id varchar(40) not null primary key, pid int, path varchar)";

		stmt.executeUpdate(sql);
		stmt.close();
	}

	public void addFileTable() throws SQLException {
		Statement stmt = this.connection.createStatement();
		String drop_sql = "DROP TABLE IF EXISTS file_events";
		stmt.executeUpdate(drop_sql);

		String sql = "CREATE TABLE file_events(id varchar(40) not null primary key, timestamp timestamp, hostname varchar(50), " +
				"action varchar(10), actorID varchar(40), objectID varchar(40)," +
				"file_path varchar)";

		stmt.executeUpdate(sql);
		stmt.close();
	}

	public void addModuleTable() throws SQLException {
		Statement stmt = this.connection.createStatement();
		String drop_sql = "DROP TABLE IF EXISTS module_events";
		stmt.executeUpdate(drop_sql);

		String sql = "CREATE TABLE module_events(id varchar(40) not null primary key, timestamp timestamp, hostname varchar(50), " +
				"action varchar(10), actorID varchar(40), objectID varchar(40), " +
				"module_path varchar)";

		stmt.executeUpdate(sql);
		stmt.close();
	}

	public void addRegistryTable() throws SQLException {
		Statement stmt = this.connection.createStatement();
		String drop_sql = "DROP TABLE IF EXISTS registry_events";
		stmt.executeUpdate(drop_sql);

		String sql = "CREATE TABLE registry_events(id varchar(40) not null primary key, timestamp timestamp, hostname varchar(50), " +
				"action varchar(10), actorID varchar(40), objectID varchar(40), " +
				"key varchar, type varchar(20), value varchar )";

		stmt.executeUpdate(sql);
		stmt.close();
	}

	public void addSocketTable() throws SQLException {
		Statement stmt = this.connection.createStatement();
		String drop_sql = "DROP TABLE IF EXISTS socket_events";
		stmt.executeUpdate(drop_sql);

		String sql = "CREATE TABLE socket_events(id varchar(40) not null primary key, timestamp timestamp, hostname varchar(50), " +
				"action varchar(10), actorID varchar(40), objectID varchar(40), " +
				"dest_ip varchar(40), dest_port int, src_ip varchar(40), src_port int, direction varchar (10), l4protocol int)";

		stmt.executeUpdate(sql);
		stmt.close();
	}




	public void closeConnection() throws SQLException {
		this.connection.close();
	}

	public void addPerson(Person person) throws SQLException {
		//query of postgresql
		String sql = "insert into person(name, identity, birthday)"
				+ "values (?,?,?)";

		PreparedStatement ps = this.connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);


		// 1 = first '?'
		ps.setString(1, person.getName());
		// 2 - second '?'
		ps.setString(2, person.getIdentity());
		// 3 = third '?'
		ps.setString(3, person.getBirthday());

		//use execute update when the database return nothing
		ps.executeUpdate();

		ResultSet generatedKeys =  ps.getGeneratedKeys();
		if(generatedKeys.next()) {
			person.setId(generatedKeys.getInt(1));
		}


	}

	public void removePerson(Person person) throws SQLException {
		String sql = "delete from person where id_person = ?";
		PreparedStatement ps = this.connection.prepareStatement(sql);
		ps.setInt(1, person.getId());
		ps.executeUpdate();


	}

	public Person getPerson(String name) throws SQLException {
		//get all persons
		ArrayList<Person> array = getAllPersons();
		for (Person person : array) {
			if(person.getName().equals(name)) {
				return person;
			}
		}
		return null;
	}

	public ArrayList<Person> getAllPersons() throws SQLException {
		ArrayList<Person> array = new ArrayList<Person>();

		//get all persons
		//query of postgresql
		ResultSet result = this.connection.prepareStatement("select * from person").executeQuery();
		while(result.next()) {
			//new Person
			Person person = new Person();
			//get column of name
			person.setName(result.getString("name"));
			person.setId(result.getInt("id_person"));
			person.setIdentity(result.getString("identity"));
			person.setBirthday(result.getString("birthday"));
			array.add(person);
		}
		result.close();
		return array;

	}
}
