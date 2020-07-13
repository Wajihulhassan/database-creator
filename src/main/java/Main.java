import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Main {

	static Connection connection;

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		if (args.length != 5) {
			System.out.println("Arguments are missing");
			return;
		}
		System.out.println(".......Starting........");

		String ecar_path = args[0];
		String TMP_PATH = args[1];
		String ecar_bro_path = args[2];
		String database_name = args[3];
		String bro_path = args[4];
		connectToSQLDB(database_name);
		//
		LoadSQLTables lst = new LoadSQLTables(ecar_path,ecar_bro_path,bro_path,TMP_PATH, connection);
		lst.createAndLoadEcarTables();
		lst.createAndLoadEcarBroTables();
		//
		connection.close();
	}
	public static void createLoadSQLTable(){

	}
	public static void connectToSQLDB(String database_name) throws ClassNotFoundException, SQLException {
		String url = "jdbc:postgresql://localhost:5432/" + database_name;
		String user = "wajih";
		String password = "corelight";
		Class.forName("org.postgresql.Driver");
		connection = DriverManager.getConnection(url, user, password);
	}
	public void loadBroLogs(String bro_path, String TMP_PATH){
		ReadBroLogs rbl = new ReadBroLogs(bro_path, TMP_PATH);
		rbl.loadDataset();
	}

}
