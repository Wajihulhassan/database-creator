import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
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
		if (args.length != 2) {
			System.out.println("Argument: path to dataset is missing ");
			return;
		}
		System.out.println(".......Starting........");

		String directory = args[0];
		TMP_PATH = args[1];
		String url = "jdbc:postgresql://localhost:5432/2019-09-25";
		String user = "wajih";
		String password = "corelight";
		Class.forName("org.postgresql.Driver");

		Connection connection = DriverManager.getConnection(url, user, password);
		CreateTables createTables = new CreateTables(connection);
		createTables.addAllTables();
		ConvertJsonIntoSQL cjs = new ConvertJsonIntoSQL(connection);
		readDirecotryAndInsertSQL(directory, cjs);
		connection.close();

	}

	/**
	 * Execute a bash command. We can handle complex bash commands including
	 * multiple executions (; | && ||), quotes, expansions ($), escapes (\), e.g.:
	 *     "cd /abc/def; mv ghi 'older ghi '$(whoami)"
	 * @param command
	 * @return true if bash got started, but your command may have failed.
	 */
	public static boolean executeBashCommand(String command) {
		boolean success = false;
		System.out.println("Executing BASH command:\n   " + command);
		Runtime r = Runtime.getRuntime();
		// Use bash -c so we can handle things like multi commands separated by ; and
		// things like quotes, $, |, and \. My tests show that command comes as
		// one argument to bash, so we do not need to quote it to make it one thing.
		// Also, exec may object if it does not have an executable file as the first thing,
		// so having bash here makes it happy provided bash is installed and in path.
		String[] commands = {"bash", "-c", command};
		try {
			Process p = r.exec(commands);

			p.waitFor();
			BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = "";

			while ((line = b.readLine()) != null) {
				System.out.println(line);
			}

			b.close();
			success = true;
		} catch (Exception e) {
			System.err.println("Failed to execute bash with command: " + command);
			e.printStackTrace();
		}
		return success;
	}

	public static String getFileName(String path) {
		if (path.contains("/")) {
			String ret_string = path.substring(path.lastIndexOf("/") + 1);
			return ret_string;
		} else if (path.contains("\\")) {
			String ret_string = path.substring(path.lastIndexOf("\\") + 1);
			return ret_string;
		}
		return "";
	}

	public static String removeExtension(String filename){
		if (filename.contains(".")) {
			return filename.substring(0,filename.lastIndexOf("."));
		}
		return "";
	}

	public static void readDirecotryAndInsertSQL(String directory, ConvertJsonIntoSQL cjs){
			String dataset_path =  directory;
			executeBashCommand("rm -rf " + TMP_PATH + "/*");
			executeBashCommand("mkdir -p " + TMP_PATH);
			try (Stream<Path> walk = Files.walk(Paths.get(dataset_path))) {
				List<Path> file_paths = walk.collect((Collectors.toList()));
				for (Path path : file_paths) {
					if (path.toString().endsWith(".json.gz")) {
						executeBashCommand("rm -rf " + TMP_PATH + "/*");
						String test_name = getFileName(path.toString());
						System.out.println(test_name);
						String copied_path = TMP_PATH + test_name;
						String final_path = TMP_PATH + removeExtension(test_name);
						executeBashCommand("cp " + path + " " + copied_path);
						executeBashCommand("gunzip -c " + copied_path + " > " + final_path);
						File[] files = new File(TMP_PATH).listFiles();
						if (files.length <= 0) {
							System.out.println("WARNING GO BACK");
							continue;
						}
						System.out.println("==========Final path: " + final_path);
						cjs.parseJsonFileWithoutOrder(final_path);
						executeBashCommand("rm -rf "+ TMP_PATH +"/*");
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

	}

}
