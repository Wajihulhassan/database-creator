import utils.Utils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LoadSQLTables {
    String ecar_path;
    String ecar_bro_path;
    String TMP_PATH;
    Connection connection;


    public LoadSQLTables(String ecar_path, String ecar_bro_path, String TMP_PATH, Connection connection) {
        this.ecar_path = ecar_path;
        this.ecar_bro_path = ecar_bro_path;
        this.TMP_PATH = TMP_PATH;
        this.connection = connection;
    }

    public void createAndLoadEcarTables() throws SQLException, ClassNotFoundException {
        CreateTables createTables = new CreateTables(connection);
		createTables.addAllTables();
		ConvertJsonIntoSQL cjs = new ConvertJsonIntoSQL(connection);
		readDirecotryAndInsertSQL(ecar_path, cjs);
		System.out.println("Size of all actors map: " + cjs.all_actors_map.size());
		cjs.addProcessEntities();
    }

    public void createAndLoadEcarBroTables() throws SQLException {
        CreateZeekTables czt = new CreateZeekTables(connection);
		czt.addBroProcessTable();
		readDirecotryAndInsertSQL(ecar_bro_path,czt);
    }

    public void readDirecotryAndInsertSQL(String directory, ConvertJsonIntoSQL cjs){
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
    public void readDirecotryAndInsertSQL(String directory, CreateZeekTables czt){
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
