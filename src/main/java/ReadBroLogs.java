import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReadBroLogs {

    String bro_directory;
    String TMP_PATH;
    List<Map<String, String[]>> all_files_map ;

    public ReadBroLogs(String bro_directory, String TMP_PATH) {
        this.bro_directory = bro_directory;
        this.TMP_PATH = TMP_PATH;
        all_files_map = new ArrayList<>();
    }
    public void loadDataset(){
        String dataset_path =  this.bro_directory ;
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

                    parseTsv(final_path);
                    Utils.executeBashCommand("rm -rf "+ TMP_PATH +"/*");
                    System.out.println("Done with \""+ test_name+"\"");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Size of List Map:" + all_files_map.size());
    }
    public void parseTsv(String fileName){
        TsvParserSettings settings = new TsvParserSettings();
        settings.getFormat().setLineSeparator("n");
        TsvParser parser = new TsvParser(settings);
        parser.beginParsing(new File(fileName));
        String[] row;
        Map<String,String[]> tmp_map = new HashMap<>();
        while ((row = parser.parseNext()) != null) {
            String id = row[0];
            tmp_map.put(id,row);
        }
        all_files_map.add(tmp_map);

    }

    public void CreateAndLoadMaps(){

    }

}
