import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReadBroLogs {

    String bro_directory;
    String TMP_PATH;
    List<Map<String, List<String>>> all_files_map ;

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
                if (path.toString().endsWith(".log.gz")) {
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

                    parseTsvManual(final_path);
                    Utils.executeBashCommand("rm -rf "+ TMP_PATH +"/*");
                    System.out.println("Done with \""+ test_name+"\"");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (Map<String,List<String>> item : all_files_map){
            System.out.println("Size of List Map:" + item.size());
        }
    }
    public void parseTsv(String fileName){
        TsvParserSettings settings = new TsvParserSettings();
        settings.getFormat().setLineSeparator("n");
        settings.setCommentCollectionEnabled(true);
        settings.setHeaderExtractionEnabled(true);
        TsvParser parser = new TsvParser(settings);
        parser.beginParsing(new File(fileName));
        String[] row;
        Map<String,String[]> tmp_map = new HashMap<>();
        while ((row = parser.parseNext()) != null) {
            System.out.println( Arrays.toString(row));
            String id = row[0];
            tmp_map.put(id,row);
        }
        //all_files_map.add(tmp_map);

    }

    public void parseTsvManual(String filename) throws IOException {
        System.out.println("Reading tsv file "+ filename);
        StringTokenizer st ;
        BufferedReader TSVFile = new BufferedReader(new FileReader(filename));
        String dataRow = TSVFile.readLine(); // Read first line.
        Map<String,List<String>> tmp_map = new HashMap<>();
        while (dataRow != null){
            if (dataRow.startsWith("#")){
                dataRow = TSVFile.readLine(); // Read next line of data.
                continue;
            }
            st = new StringTokenizer(dataRow,"\t");
            List<String>dataArray = new ArrayList<String>() ;
            while(st.hasMoreElements()){
                dataArray.add(st.nextElement().toString());
            }
            dataRow = TSVFile.readLine(); // Read next line of data.
            tmp_map.put(dataArray.get(1),dataArray);
        }
        all_files_map.add(tmp_map);
        TSVFile.close();
    }

    public void CreateAndLoadMaps(){

    }

}
