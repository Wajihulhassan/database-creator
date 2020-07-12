import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {

	static String TMP_PATH;
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		if (args.length != 3) {
			System.out.println("Arguments are missing");
			return;
		}
		System.out.println(".......Starting........");

		String directory = args[0];
		TMP_PATH = args[1];
		String net_json_directory = args[2];
		String database_name = args[3];
		String bro_directory= args[4];

		ReadBroLogs rbl = new ReadBroLogs(bro_directory, TMP_PATH);
		rbl.loadDataset();


		String url = "jdbc:postgresql://localhost:5432/" + database_name;
		String user = "wajih";
		String password = "corelight";
		Class.forName("org.postgresql.Driver");



		Connection connection = DriverManager.getConnection(url, user, password);

//		CreateZeekTables czt = new CreateZeekTables(connection);
//		czt.addBroProcessTable();
//		readDirecotryAndInsertSQL(net_json_directory,czt);

//		CreateTables createTables = new CreateTables(connection);
//		createTables.addAllTables();
//		ConvertJsonIntoSQL cjs = new ConvertJsonIntoSQL(connection);
//		readDirecotryAndInsertSQL(directory, cjs);
//		cjs.addProcessEntities();
//		connection.close();

	}



	public static void readDirecotryAndInsertSQL(String directory, ConvertJsonIntoSQL cjs){
			String dataset_path =  directory;
			Utils.executeBashCommand("rm -rf " + TMP_PATH + "/*");
			Utils.executeBashCommand("mkdir -p " + TMP_PATH);
			try (Stream<Path> walk = Files.walk(Paths.get(dataset_path))) {
				List<Path> file_paths = walk.collect((Collectors.toList()));
				for (Path path : file_paths) {
					if (path.toString().endsWith(".json.gz")) {
						Utils.executeBashCommand("rm -rf " + TMP_PATH + "/*");
						String test_name = Utils.getFileName(path.toString());
						System.out.println(test_name);
						String copied_path = TMP_PATH + test_name;
						String final_path = TMP_PATH + Utils.removeExtension(test_name);
						Utils.executeBashCommand("cp " + path + " " + copied_path);
						Utils.executeBashCommand("gunzip -c " + copied_path + " > " + final_path);
						File[] files = new File(TMP_PATH).listFiles();
						if (files.length <= 0) {
							System.out.println("WARNING GO BACK");
							continue;
						}
						System.out.println("==========Final path: " + final_path);
						cjs.parseJsonFileWithoutOrder(final_path);
						Utils.executeBashCommand("rm -rf "+ TMP_PATH +"/*");
						System.out.println("Done with \""+ test_name+"\"");
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

	}
	public static void readDirecotryAndInsertSQL(String directory, CreateZeekTables czt){
		String dataset_path =  directory;
		Utils.executeBashCommand("rm -rf " + TMP_PATH + "/*");
		Utils.executeBashCommand("mkdir -p " + TMP_PATH);
		try (Stream<Path> walk = Files.walk(Paths.get(dataset_path))) {
			List<Path> file_paths = walk.collect((Collectors.toList()));
			for (Path path : file_paths) {
				if (path.toString().endsWith(".json.gz")) {
					Utils.executeBashCommand("rm -rf " + TMP_PATH + "/*");
					String test_name = Utils.getFileName(path.toString());
					System.out.println(test_name);
					String copied_path = TMP_PATH + test_name;
					String final_path = TMP_PATH + Utils.removeExtension(test_name);
					Utils.executeBashCommand("cp " + path + " " + copied_path);
					Utils.executeBashCommand("gunzip -c " + copied_path + " > " + final_path);
					File[] files = new File(TMP_PATH).listFiles();
					if (files.length <= 0) {
						System.out.println("WARNING GO BACK");
						continue;
					}
					System.out.println("==========Final path: " + final_path);
					czt.parseJsonFileWithoutOrder(final_path);
					Utils.executeBashCommand("rm -rf "+ TMP_PATH +"/*");
					System.out.println("Done with \""+ test_name+"\"");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
