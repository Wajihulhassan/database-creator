import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class CreateTables{

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
		System.out.println("Done Creating Tables");
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
}
