import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class LogReader {


    private static final String FILE_ENCODING_TYPE = "UTF-8";
    private static final String RECORD_IDENTIFIER = "TID:";
    private static final String RECORD_CONTENT_REGEX = "\\s(.*?)\\s+";
    private static final String DURATION_SPLITTER = " ";
    private static final String QUERY_SPLITTER = " - ";

    private String extractDuration(String duration) {
        return "executed in " + duration + " msec";
    }

    void readLogContent(String filePath) {


//        String rawLogContent = "";
//
        List<String> rawRecordsList = new ArrayList<>();
        List<String> dbQueryList = new ArrayList<>();
        List<String> executedList = new ArrayList<>();
        List<Integer> timeList = new ArrayList<>();

        Map<Integer, String> filter = new HashMap<>();
        Map<String, Integer> queryExecutionTimeMap = new HashMap<>();

        Matcher rawRecordMatcher, queryMatcher, select;

//        Pattern pattern = Pattern.compile("executed in " + "\\d+" + " msec");
        Pattern pattern = Pattern.compile(extractDuration("\\d+"));
        Pattern queryPattern;
        Pattern selectPattern;

        PrintWriter timeBasedFileWriter, dbQueryCountWriter, spyLogsWriter, logSummaryWriter;

        try {
            //writer = new PrintWriter("/Users/sarankimagenthirarajah/Desktop/Log/Sample/writer.txt", FILE_ENCODING_TYPE);
            spyLogsWriter = new PrintWriter("/Users/sarankimagenthirarajah/Desktop/Log/Sample/spy_logs.txt", FILE_ENCODING_TYPE);
            //dbQueryWriter = new PrintWriter("/Users/sarankimagenthirarajah/Desktop/Log/Sample/db_query.txt", FILE_ENCODING_TYPE);
            dbQueryCountWriter = new PrintWriter("/Users/sarankimagenthirarajah/Desktop/Log/Sample/query_count.txt", FILE_ENCODING_TYPE);
            logSummaryWriter = new PrintWriter("/Users/sarankimagenthirarajah/Desktop/Log/Sample/list.txt", FILE_ENCODING_TYPE);

            //Read the spy log file
            byte[] fileAsByteArray = Files.readAllBytes(Paths.get(filePath));
            String rawLogContent = new String(fileAsByteArray, StandardCharsets.US_ASCII);

            //Filter the records that has "{executed in {} msec}" pattern in it
            for (String rawRecord : rawLogContent.split(RECORD_IDENTIFIER)) {
                rawRecordMatcher = pattern.matcher(rawRecord);

                if (rawRecordMatcher.find()) {
                    rawRecordsList.add(rawRecord);
                    //System.out.println("rawRecordMatcher.group().split(DURATION_SPLITTER)[2]): " + rawRecordMatcher.group().split(DURATION_SPLITTER)[2]);
                    //timelist value sample : 1
                    timeList.add(Integer.parseInt(rawRecordMatcher.group().split(DURATION_SPLITTER)[2]));
                }
            }

            Map<Integer, Long> result = timeList.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

            System.out.println(rawRecordsList.size());
            spyLogsWriter.println("\t\t\t\t\t\t\t\t\tSPY LOG ANALYSIS\n");

            for (Map.Entry<Integer, Long> entry : result.entrySet()) {
                //Create files for each duration value
                //timeBasedFileWriter = new PrintWriter("/Users/sarankimagenthirarajah/Desktop/Log/Sample/" + entry.getKey() + ".txt", FILE_ENCODING_TYPE);

                // Time: 0 ms : Count = 1
                // Time: 1 ms : Count = 7
                //Time based file
                //timeBasedFileWriter.println("Time: " + entry.getKey() + " ms : Count = " + entry.getValue() + "\n");

                // Complete file
                spyLogsWriter.println("Time: " + entry.getKey() + " ms : Count = " + entry.getValue() + "\n");

                // filter out the record which has been executed with the specified time duration
                for (String val : rawRecordsList) {
                    if (val.contains(extractDuration(entry.getKey().toString()))) {
//                    if (val.contains("executed in " + entry.getKey() + " msec")) {
                        //timeBasedFileWriter.println(RECORD_IDENTIFIER + val);
                        spyLogsWriter.println(RECORD_IDENTIFIER + val);
                    }
                }
                //timeBasedFileWriter.println("\n");
                spyLogsWriter.println("----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n");

                //timeBasedFileWriter.close();
            }
            spyLogsWriter.close();

            for (String val : rawRecordsList) {
                dbQueryList.add((RECORD_IDENTIFIER + val).split(QUERY_SPLITTER)[1].split("\\{executed in")[0]);
            }

            Map<String, Long> queryCountMap = dbQueryList.stream()
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

            Map<String, Long> sortedByCount = queryCountMap.entrySet()
                    .stream()
                    .sorted((Map.Entry.<String, Long>comparingByValue().reversed()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));


            Long queryFrequency = 0L;
            // Write the query with its frequency to the file
            // does not allow duplicate queries
            for (Map.Entry<String, Long> entry : sortedByCount.entrySet()) {
                dbQueryCountWriter.println(entry.getKey().trim() + "\nCount = " + entry.getValue() + "\n");
                queryFrequency += entry.getValue();
            }
            System.out.println("Total: " + queryFrequency);
            dbQueryCountWriter.close();

            // Remove duplicate queries
//            Set<String> querySet = new LinkedHashSet<>();
//            sortedByCount.entrySet().forEach(entry -> {
//                querySet.add(entry.getKey());
//            });

            System.out.println("Query List content : " + rawRecordsList.size());
            System.out.println("Selected query List content : " + sortedByCount.size());

            Map<String, Map<Integer, Integer>> finalQueryMap = new LinkedHashMap<>();
            Map<Integer, Integer> queryFrequencyMap;

            int times;
            int flag = 0;
            for (String query : sortedByCount.keySet()) {
                queryFrequencyMap = new LinkedHashMap<>();
                //int totalDuration = 0;
                //System.out.println("Query : " + query);
                for (String record : rawRecordsList) {
                    if (record.contains(query)) {
                        times = Integer.parseInt(record.split("\\{executed in")[1].split(" msec")[0].trim());
                        //System.out.println("Record : "+ record);
                        //System.out.println("Duration : " + times);
                        if (!queryFrequencyMap.containsKey(times)) {
                            queryFrequencyMap.put(times, 1);
                        } else {
                            queryFrequencyMap.put(times, queryFrequencyMap.get(times) + 1);
                        }
                        //totalDuration = totalDuration + Integer.parseInt(record.split("\\{executed in")[1].split(" msec")[0].trim());
                    }
                }
                flag = flag+1;
                System.out.println("Count: "+flag);
                //System.out.println("----------------------------------------------------------------------------------------------");
                finalQueryMap.put(query, queryFrequencyMap);
                //queryExecutionTimeMap.put(query, totalDuration);
            }

/*
            Map<String, Integer> sortedByDuration = queryExecutionTimeMap.entrySet()
                    .stream()
                    .sorted((Map.Entry.<String, Integer>comparingByValue().reversed()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

            for (Map.Entry<String, Integer> entry : sortedByDuration.entrySet()) {
                q.println(entry.getKey());
                q.println("Total Duration = " + entry.getValue() + " ms\n\n");
            }
            //Arrays.stream(queryList).filter(fields::containsKey).findAny();
            q.close();
*/
            int queryCount = 1;
            System.out.println("FREQUENCY");
            logSummaryWriter.println("\n-----------------------------------------------------------------------------------------------------Complete Log Summary---------------------------------------------------------------------------------------------------------------------\n");
            for (String query : finalQueryMap.keySet()) {
                int duration = 0;
                int count = 0;
                logSummaryWriter.println("Query No: "+ queryCount);
                logSummaryWriter.println(query.trim() + "\n");
                logSummaryWriter.println("Time(msec)\tFrequency");
                System.out.println(query.trim() + "\n");
                System.out.println("Time(msec)\tFrequency");
                Map<Integer, Integer> innerMap = finalQueryMap.get(query);
                for (Integer time : innerMap.keySet()) {
                    logSummaryWriter.println("   " +time + "\t\t\t" + innerMap.get(time));
                    System.out.println("\t" + time + "\t\t\t" + innerMap.get(time));
                    duration = duration + (time * innerMap.get(time));
                    count = count + innerMap.get(time);
                }
                logSummaryWriter.println("Total Duration = " + duration);
                logSummaryWriter.println("Total Occurances of the query execution = " + count);
                logSummaryWriter.println("----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n");
                System.out.println("\nTotal Duration = " + duration);
                System.out.println("Total Occurances of the query execution = " + count);
                System.out.println("---------------------------------------------------------------------------------------\n");
                queryCount = queryCount+1;
            }
            logSummaryWriter.close();
        } catch (IOException e) {

            e.printStackTrace();
        }
        System.out.println("DONE!!!!");
    }
}





