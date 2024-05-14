package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.here.commons.utils.JsonUtils;
import com.here.umap.models.UMapTask;
import com.here.umap.models.UMapTaskStatus;
import com.here.xyz.XYZService;
import com.here.xyz.exception.XYZParseException;
import com.here.xyz.exception.XYZServiceExcpetion;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

@Slf4j
public class Main {

    private static final String ES_HOST = "es_host";
    private static final String INDEX_NAME = "index_name";
    private static final String SPACE_NAME = "space_name";
    private static final String DATASOURCE_DRIVER_CLASS_KEY = "datasource_driver_class_key";
    private static final String DATASOURCE_CONNECTION_URL_KEY = "datasource_connection_url_key";
    private static final String DATASOURCE_USERNAME_KEY = "datasource_username_key";
    private static final String DATASOURCE_PASSWORD_KEY = "datasource_password_key";
    private static final String HIKARI_CONNECTION_POOL_TIMEOUT_KEY = "hikari_connection_pool_timeout_key";
    private static final String HIKARI_CONNECTION_POOL_SIZE_KEY = "hikari_connection_pool_size_key";
    private static final String HIKARI_CONNECTION_POOL_MINIMUM_IDLE_KEY = "hikari_connection_pool_minimum_idle_key";
    private static final String HIKARI_CONNECTION_POOL_IDLE_TIMEOUT_KEY = "hikari_connection_pool_idle_timeout_key";

    private static final String BASE_URL = "base_url";
    private static final String AUTH_TOKEN = "auth_token";
    private static final String STREAM_ID = "stream_id";
    private static final String ENV = "PRD";
    private static HikariDataSource ds;
    private static Properties config;

    private static final String BASE_PATH = "src/test/resources/";

    private static XYZService xyzService;

    static {
        xyzService = new XYZService();
        config = new Properties();
        if(ENV.equalsIgnoreCase("prd")){
            config.setProperty(ES_HOST, "https://vpc-here-maptask-prd-5upn6tiuiozc5pcizcu25jdfma.us-east-1.es.amazonaws.com");
            config.setProperty(INDEX_NAME, "cs_mcpt_utm_maptask_alias_prd");
            config.setProperty(SPACE_NAME, "utm-prd:maptask");
            config.setProperty(DATASOURCE_DRIVER_CLASS_KEY, "org.postgresql.Driver");
            config.setProperty(DATASOURCE_CONNECTION_URL_KEY, "jdbc:postgresql://task-mgmt-prd.cluster-cdtqvazz0g7c.eu-west-1.rds.amazonaws.com:5432/task_mgmt");
            config.setProperty(DATASOURCE_USERNAME_KEY, "utm_ro_user");
            config.setProperty(DATASOURCE_PASSWORD_KEY, "byHqNnpMJjuVfyM");
            config.setProperty(HIKARI_CONNECTION_POOL_TIMEOUT_KEY, "30000");
            config.setProperty(HIKARI_CONNECTION_POOL_SIZE_KEY, "50");
            config.setProperty(HIKARI_CONNECTION_POOL_MINIMUM_IDLE_KEY, "5");
            config.setProperty(HIKARI_CONNECTION_POOL_IDLE_TIMEOUT_KEY, "600000");
            config.setProperty(BASE_URL, "https://naksha.ext.mapcreator.here.com/hub");
            config.setProperty(AUTH_TOKEN, "upm.MnDnqtYasthgUwhM");
            config.setProperty(STREAM_ID, UUID.randomUUID().toString());

        } else if (ENV.equalsIgnoreCase("e2e")){
            config.setProperty(ES_HOST, "https://vpc-here-maptask-e2e-m4hbecrfyrmeqflbla2uwus7zm.us-east-1.es.amazonaws.com");
            config.setProperty(INDEX_NAME, "cs_mcpt_utm_maptask_alias_e2e");
            config.setProperty(SPACE_NAME, "utm-e2e:maptask");
            config.setProperty(DATASOURCE_DRIVER_CLASS_KEY, "org.postgresql.Driver");
            config.setProperty(DATASOURCE_CONNECTION_URL_KEY, "jdbc:postgresql://task-mgmt-e2e.cluster-ccmrakfzvsi3.us-east-1.rds.amazonaws.com:5432/task_mgmt");
            config.setProperty(DATASOURCE_USERNAME_KEY, "utm_ro_user");
            config.setProperty(DATASOURCE_PASSWORD_KEY, "=rh2NwDHaA&GmL_X");
            config.setProperty(HIKARI_CONNECTION_POOL_TIMEOUT_KEY, "30000");
            config.setProperty(HIKARI_CONNECTION_POOL_SIZE_KEY, "50");
            config.setProperty(HIKARI_CONNECTION_POOL_MINIMUM_IDLE_KEY, "5");
            config.setProperty(HIKARI_CONNECTION_POOL_IDLE_TIMEOUT_KEY, "600000");
            config.setProperty(BASE_URL, "https://naksha-e2e.ext.mapcreator.here.com/hub");
            config.setProperty(AUTH_TOKEN, "upm.FNEbFrASURTLjaBv");
            config.setProperty(STREAM_ID, UUID.randomUUID().toString());
        }
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();

        HashMap<String, List<String>> sortedIds = sortIdsBasedOnStatus(getDataFromFile("src/test/resources/prd/i2/tasks_not_in_DB_1713451793833.txt"));
        log.info("SOrted IDs : {}", sortedIds);
        long endTime = System.currentTimeMillis() - startTime;



        List<String> openIds = sortedIds.get("OPEN");
        openIds = getIdsStillInOpenState(openIds);
        log.info(openIds.toString());
        log.info("Time taken for cleaning up one file : {}s", TimeUnit.MILLISECONDS.toSeconds(endTime));

    }

    @SneakyThrows
    private static HashMap<String, List<String>> sortIdsBasedOnStatus(List<String> ids){
        List<List<String>> subSets = ListUtils.partition(ids, 50);
        HashMap<String, List<String>> sortedMaps = new HashMap<>();
        for(List<String> subSet: subSets){
            List<UMapTask> mapTasks = xyzService.getDocuments(config.getProperty(BASE_URL), subSet, config.getProperty(SPACE_NAME), config.getProperty(AUTH_TOKEN), config.getProperty(STREAM_ID), UMapTask.class);
            for (UMapTask mapTask : mapTasks){
                String mapTaskStatus = mapTask.getProperties().getStatus();
                if(sortedMaps.containsKey(mapTaskStatus))
                    sortedMaps.get(mapTaskStatus).add(mapTask.getId());
                else
                    sortedMaps.put(mapTaskStatus, new ArrayList<>(Arrays.asList(mapTask.getId())));
            }
        }

        return sortedMaps;
    }

    private static void deleteIdsFromFile(List<String> idsToDelete){
        List<List<String>> subSets = ListUtils.partition(idsToDelete, 1000);
        int listCounter = 0;
        for(List<String> subList: subSets){
            log.info("list counter : {}", listCounter);
            deleteIdsFromES(subList);
            listCounter++;
        }
    }

    @SneakyThrows
    private static List<String> deleteIdsFromES(List<String> idsToDelete){
        List<String> idsPresentInDB = new ArrayList<>();
        String urlToDeleteESRecord = "%s/%s/_doc/%s";
        UMapTask mapTask = null;
        for(String id : idsToDelete){
            mapTask = xyzService.getDocument(config.getProperty(BASE_URL), id,
                    config.getProperty(SPACE_NAME), config.getProperty(AUTH_TOKEN), config.getProperty(STREAM_ID), UMapTask.class);

            if(mapTask == null)
                deleteRecordFromES(String.format(urlToDeleteESRecord, config.get(ES_HOST), config.get(INDEX_NAME), id));
            else
                idsPresentInDB.add(id);
        }

        return idsPresentInDB;
    }

    @SneakyThrows
    private static List<String> idsNotPresentInDB(List<String> ids){
        UMapTask mapTask;
        List<String> idsInDB = new ArrayList<>();

        try {
            for(String id : ids){
                mapTask = xyzService.getDocument(config.getProperty(BASE_URL), id,
                        config.getProperty(SPACE_NAME), config.getProperty(AUTH_TOKEN), config.getProperty(STREAM_ID), UMapTask.class);

                if(mapTask != null)
                    idsInDB.add(id);
            }
        } catch (XYZServiceExcpetion | XYZParseException e) {
            throw new RuntimeException(e);
        }
        return idsInDB;
    }

    public static String deleteMultipleRecordFromES(List<String> ids){
        //{{host_url}}cs_mcpt_utm_maptask_alias_{{env}}/_doc/_search?scroll=1m
        String urlToDeleteESRecord = "%s/%s/_doc/_delete_by_query";
        String queryForBulkDelete = """
                {"query" : {"terms" : { "_id" :  %s  } } }
                """;

        return sendRequest(String.format(urlToDeleteESRecord, config.getProperty(ES_HOST), config.getProperty(INDEX_NAME)),
                String.format(queryForBulkDelete, ids.toString()));
    }

    public static void deleteRecordFromES(String recordUrl){
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {

            HttpDelete httpDelete = new HttpDelete(recordUrl);
            System.out.println("Executing request " + httpDelete.getRequestLine());

            // Create a custom response handler
            ResponseHandler<String> responseHandler = response -> {
                int status = response.getStatusLine().getStatusCode();
                if (status >= 200 && status < 300) {
                    HttpEntity entity = response.getEntity();
                    return entity != null ? EntityUtils.toString(entity) : null;
                } else{
                    log.info("ID not deleted from ES: {}", recordUrl);
                    return null;
                }
            };
            String responseBody = httpclient.execute(httpDelete, responseHandler);
            System.out.println(responseBody);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> fetchAllIdsNotPresent(List<String> mainList, String fileName, int counter){
        log.info("fetching data for file: {} and counter : {}", fileName, counter);
        fileName = fileName + counter + ".txt";
        return getIdsNotPresentInMainList(mainList, getDataFromFile(fileName));
    }

    private static List<String> getIdsNotPresentInMainList(List<String> mainList, List<String> secondQueue){
        List<List<String>> subSets = ListUtils.partition(secondQueue, 10000);
        log.info("Size of main subsets: {}", subSets.size());
        List<String> idsNotPresent = new ArrayList<>();
        for(List<String> subSet: subSets){
            idsNotPresent.addAll(subSet.parallelStream().filter(taskId -> !mainList.contains(taskId)).toList());
        }
        return idsNotPresent;
    }

    private static void findMismatchedIds(){
        long timeStamp = System.currentTimeMillis();
        Set<String> idsInDB = getIdsInDB(config);
        writeDataToFile(idsInDB, "taskIds_DB", timeStamp);

        Set<String> idsInES = getIdsInES(config);
        writeDataToFile(idsInES, "taskIds_ES", timeStamp);

        log.info("Data successfully fetched from both places!");

        ArrayList<String> duplicates = new ArrayList<>(idsInDB);
        duplicates.retainAll(idsInES);

        ArrayList<String> idsNotPresentInES = new ArrayList<>(idsInDB);
        idsNotPresentInES.removeAll(idsInES);

        ArrayList<String> idsNotPresentInDB = new ArrayList<>(idsInES);
        idsNotPresentInDB.removeAll(idsInDB);

        log.info("Size of tasks in ES : {}, Size of tasks in DB : {}", idsInES.size(), idsInDB.size());
        log.info("Size of duplicates : {}, Size of tasks not present in ES : {}, Size of tasks not present in DB : {}", duplicates.size(), idsNotPresentInES.size(), idsNotPresentInDB.size());

        log.info("Writing ids which a not present in ES!");
        writeDataToFile(idsNotPresentInES, "tasks_not_in_ES", timeStamp);

        log.info("Writing ids which a not present in DB!");
        writeDataToFile(idsNotPresentInDB, "tasks_not_in_DB", timeStamp);
    }

    @SneakyThrows
    private static void writeDataToFile(Collection<String> data, String fileName, long timeStamp){
        fileName = BASE_PATH + ENV.toLowerCase() + "/" + fileName+ "_"+timeStamp+".txt";

        try (BufferedWriter br = new BufferedWriter(new FileWriter(fileName))){
            for (String str : data) {
                br.write(str + System.lineSeparator());
            }
        }
    }

    @SneakyThrows
    private static void breakFileIntoSubfilesAndWrite(Collection<String> data, String fileName, int numberOfRecordsInOneFile){
        List<List<String>> subSets = ListUtils.partition(data.stream().toList(), numberOfRecordsInOneFile);
        int counter=1;
        for(List<String> subSet: subSets){
            String relevantFileName = fileName +"_"+ counter +".txt";
            try (BufferedWriter br = new BufferedWriter(new FileWriter(relevantFileName))){
                for (String str : subSet) {
                    br.write(str + System.lineSeparator());
                }
            }
            counter++;
        }

    }

    private static List<String> getIdsStillInOpenState(List<String> taskIds) {
        XYZService xyzService = new XYZService();
        List<List<String>> subSets = ListUtils.partition(taskIds, 50);
        List<String> idsInOpenState = new ArrayList<>();
        for(List<String> ids : subSets){
            List<UMapTask> mapTasks = null;
            try {
                mapTasks = xyzService.getDocuments(config.getProperty(BASE_URL), ids, config.getProperty(SPACE_NAME), config.getProperty(AUTH_TOKEN), config.getProperty(STREAM_ID), UMapTask.class);
            } catch (XYZServiceExcpetion | XYZParseException e ) {
                log.error("", e);
            }
            List<String> mapTaskIds = mapTasks.parallelStream().filter(uMapTask -> uMapTask.getProperties().getStatus().equalsIgnoreCase("OPEN")).map(UMapTask::getId).toList();
            idsInOpenState.addAll(mapTaskIds);
        }
        return idsInOpenState;
    }

    private static String getScrollIdRequestBody(String scrollId){
        return String.format("{ \"scroll\": \"1m\", \"scroll_id\": \"%s\"}", scrollId);
    }
    @SneakyThrows
    private static Set<String> getIdsInES(Properties properties){
        String urlForSearch = String.format("%s/%s/_doc/_search?scroll=1m", properties.get(ES_HOST), properties.get(INDEX_NAME));
        String urlForScroll = String.format("%s/_search/scroll", properties.get(ES_HOST));

        Set<String> taskIds = new HashSet<>();
        String scrollId = "";
        int remainingTasks = -1, loopCount = 0, totalTasks = 0;
        String queryResponse = "";

        String body = "{\"size\": 100000}";
        while(remainingTasks != 0){
            if(remainingTasks == -1)
                queryResponse = sendRequest(urlForSearch, body);
            else
                queryResponse = sendRequest(urlForScroll, getScrollIdRequestBody(scrollId));

            JsonNode node = JsonUtils.mapper.readTree(queryResponse);
            scrollId = node.get("_scroll_id").asText();
            totalTasks = totalTasks == 0 ? node.get("hits").get("total").get("value").asInt() : totalTasks;
            List<JsonNode> datasets = StreamSupport.stream(node.get("hits").get("hits").spliterator(), false).toList();
            remainingTasks = remainingTasks == -1 ? totalTasks-datasets.size() : remainingTasks- datasets.size();

            for (JsonNode hit : datasets)
            {   taskIds.add(hit.get("_id").asText());   }

            loopCount++;
            log.info("LoopCount : {}, Tasks remaining : {}, Total Tasks : {}", loopCount, remainingTasks, totalTasks);
        }
        return taskIds;

    }
    private static List<String> getDataFromFile(String fileName){
        StringBuffer sb=new StringBuffer();
        try
        {
            File file=new File(fileName);    //creates a new file instance
            FileReader fr=new FileReader(file);   //reads the file
            BufferedReader br=new BufferedReader(fr);  //creates a buffering character input stream
                //constructs a string buffer with no characters
            String line;
            while((line=br.readLine())!=null)
            {
                sb.append(line);      //appends line to string buffer
                sb.append("\n");     //line feed
            }
            fr.close();    //closes the stream and release the resources
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }

        return List.of(sb.toString().split("\\n"));
    }
    private static String sendRequest(String url, String body) {
        String responseString = "";
        HttpClient httpClient = HttpClientBuilder.create().build();
        try {
            HttpPost request = new HttpPost(url);
            StringEntity params = new StringEntity(body);
            request.addHeader("content-type", "application/json");
            request.setEntity(params);
            HttpResponse response = httpClient.execute(request);
            responseString = EntityUtils.toString(response.getEntity());
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        return responseString;
    }
    @SneakyThrows
    public static Set<String> getIdsInDB(Properties properties){
        Set<String> maptaskIds = new HashSet<>();
        String query = "select jsondata ->> 'id'  as maptaskid from xyz_spaces.\""+properties.getProperty(SPACE_NAME)+"\" \n" +
                "where (jsondata -> 'properties' -> '@ns:com:here:xyz' -> 'tags') ? 'status_open'\n" +
                "and (jsondata -> 'properties' -> '@ns:com:here:xyz' -> 'tags')::jsonb ?| array['otm_maptask','utm_maptask']";

        try (Connection conn = getConnection(properties);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            // Extract data from result set
            while (rs.next()) {
                maptaskIds.add(rs.getString("maptaskid"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return maptaskIds;
    }
    @SneakyThrows
    private static Connection getConnection(Properties properties){
        HikariConfig config = new HikariConfig();

        config.setDriverClassName(properties.getProperty(DATASOURCE_DRIVER_CLASS_KEY));
        config.setJdbcUrl(properties.getProperty(DATASOURCE_CONNECTION_URL_KEY));
        config.setUsername(properties.getProperty(DATASOURCE_USERNAME_KEY));
        config.setPassword(properties.getProperty(DATASOURCE_PASSWORD_KEY));
        config.setConnectionTimeout(Long.parseLong(properties.getProperty(HIKARI_CONNECTION_POOL_TIMEOUT_KEY)));
        config.setMaximumPoolSize(Integer.parseInt(properties.getProperty(HIKARI_CONNECTION_POOL_SIZE_KEY)));
        config.setMinimumIdle(Integer.parseInt(properties.getProperty(HIKARI_CONNECTION_POOL_MINIMUM_IDLE_KEY)));
        config.setIdleTimeout(Long.parseLong(properties.getProperty(HIKARI_CONNECTION_POOL_IDLE_TIMEOUT_KEY)));

        ds = new HikariDataSource(config);
        return  ds.getConnection();
    }
}

/*


      List<String> idsToDelete = getDataFromFile("src/test/resources/e2e/i2/ids_to_delete_from_11.txt");
        idsToDelete = idsToDelete.stream().map(id -> '"' + id + '"').collect(Collectors.toList());


        List<List<String>> subSets = ListUtils.partition(idsToDelete, 10000);
        for(List<String> subSet : subSets){
            String dataReturned =  deleteMultipleRecordFromES(subSet);
            log.info("Data returned : {} ", dataReturned);
        }



List<String> idsNotInDB = getDataFromFile("src/test/resources/e2e/i2/ids_to_delete_from_11.txt");
        List<String> idsFoundInDb = new ArrayList<>();
        List<List<String>> subSets = ListUtils.partition(idsNotInDB, 500);
        int counter = 1;
        for(List<String> subSet: subSets){
            idsFoundInDb.addAll(idsNotPresentInDB(subSet));
            log.info("Counter: {}, Ids found in DB : {} ", counter, idsFoundInDb);
            counter++;
        }
//breakFileIntoSubfilesAndWrite(idsToDelete, "src/test/resources/e2e/ids_to_delete_from", 10000);

{
                ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
                executor.submit(() -> {
                    log.info("Starting execution for thread with id: {}", Thread.currentThread().getId());
                     idsNotPresentInEachSubset.addAll(taskIds.parallelStream().filter(taskId -> mainQueue.contains(taskId)).toList());
                });
            }



                 List<String> idsNotInEs = new ArrayList<>();

        for(int i = 0; i <= 22; i++){
            idsNotInEs.addAll(fetchAllIdsNotPresent(idsInDB, "src/test/resources/taskIds_ES_prd_1712738857378_", i));
        }
This is another comment
 */

