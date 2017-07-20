package service;

import bookkeepertool.LogMetadata;
import hdfstool.HDFSException;
import io.pravega.common.cluster.Host;
import io.pravega.common.segment.SegmentToContainerMapper;

import lombok.Setter;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import record.ActiveTxnRecord;
import record.CompletedTxnRecord;
import record.SegmentRecord;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;


/**
 * Tool to get the stream's information.
 */
public class StreamStat {

    // region configuration
    private StreamStatConfigure conf;

    // endregion

    // region properties read
    private Integer containerCount;
    private String clusterName;
    private Map<String,Long> segmentNameToIdMap;
    private Map<Integer,String> containerToSegmentNameMap;
    private List<LogMetadata> logs;

    //endregion

    /**
     * main function reading the command line arguments and set a stream stat instance
     */
    public static void main(String[] args) {
        // open properties
        Properties pravegaProperties = new Properties();

        try (InputStream input = new FileInputStream(
                System.getProperty("pravega.configurationFile").substring("file:".length()))){
            pravegaProperties.load(input);
        } catch (Exception e){
            pravegaProperties.clear();
        }

        // get the command line options

        Options options = getOptions();
        CommandLine cmd = null;
        try {
            cmd = parseCommandLineArgs(options, args);
            if (cmd.hasOption('h')) throw new ParseException("Help");
        } catch (ParseException e) {
            System.out.format("%s.%n", e.getMessage());
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("StreamStat", options);
            System.exit(1);
        }

        // set configures
        String scopedStreamName = pravegaProperties.getProperty("streamName", Constants.DEFAULT_STREAM_NAME);
        String[] parts = scopedStreamName.split("/");

        if (parts.length != 2 && !cmd.hasOption('i')) {
            ExceptionHandler.INVALID_STREAM_NAME.apply();
        }

        String streamName=parts[1],
                scopeName=parts[0];

        if (cmd.hasOption('i')) {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in))){

                System.out.print("Enter scope name > ");
                scopeName = reader.readLine();
                System.out.print("Enter stream name > ");
                streamName = reader.readLine();


            } catch (IOException e) {
                e.printStackTrace();
                ExceptionHandler.CANNOT_READ_STREAM_NAME.apply();
            }
        }

        final String zkURL = pravegaProperties.getProperty("pravegaservice.zkURL",Constants.DEFAULT_ZK_URL);
        final String hdfsURL = pravegaProperties.getProperty("hdfs.hdfsUrl",Constants.DEFAULT_HDFS_URL);

        StreamStatConfigure conf = new StreamStatConfigure();

        conf.setScope(scopeName);
        conf.setStream(streamName);
        conf.setZkURL(zkURL);
        conf.setHdfsURL(hdfsURL);
        if (cmd.hasOption('a')) conf.setSimple(false);
        if (cmd.hasOption('s')) conf.setBookkeeper(false);
        if (cmd.hasOption('d')) conf.setData(true);
        if (cmd.hasOption('c')) conf.setCluster(true);
        if (cmd.hasOption('t')) conf.setTxn(true);
        if (cmd.hasOption('f')) conf.setFence(true);
        if (cmd.hasOption('e')) {
            conf.setExplicit(true);
            conf.setFence(false);
        }
        if (cmd.hasOption('l')) conf.setLog(true);
        // service start

        StreamStat.createStreamStat(conf)
                .printZKData()
                .printHDFSData()
                .printBKData()
                .summarize()
                .end();

    }

    /**
     * Take the configurations and initialize some data structure
     * @param conf pass the configurations to instance
     */
    private StreamStat(StreamStatConfigure conf) {
        this.conf = conf;
        this.containerCount = Constants.DEFAULT_CONTAINER_COUNT;
        this.containerToSegmentNameMap = new HashMap<>();
        this.logs = new ArrayList<>();
        this.segmentNameToIdMap = new HashMap<>();
    }

    private static StreamStat createStreamStat(StreamStatConfigure conf) {
        return new StreamStat(conf);
    }

    /**
     * Print the data from zoo keeper, including:
     * <ul>
     *     <li>Cluster information in "/cluster" </li>
     *     <li>List stream segments in "/store/${scope}/${stream}/segment" </li>
     *     <li>Container's log metadata </li>
     * </ul>
     * @return this
     */
    private StreamStat printZKData(){

        ZKHelper zk = ZKHelper.create(this.conf.zkURL, this.conf.scope, this.conf.stream);

        // region cluster information

        if (conf.cluster) {
            PrintHelper.printHead("Information for cluster");
            PrintHelper.println();
        }

        clusterName = zk.getClusterName();

        // display all the controllers and their uri
        if (conf.cluster) PrintHelper.format("Controllers: %n");

        List<String> controllers = zk.getControllers();
        for (String controller:
                controllers) {
            String[] parts = controller.split(":");
            if (parts.length >= 2)
                if (conf.cluster) PrintHelper.format("Controller[%s:%s]%n", parts[0], parts[1]);
        }

        // display all the segment stores and containers they hold.
        Map<Host,Set<Integer>> hostMap = zk.getCurrentHostMap();
        if (conf.cluster) PrintHelper.format("%nSegment Container mappings:%n");
        for (Host host:
                hostMap.keySet()) {
            if (conf.cluster) PrintHelper.format("Segment Store[%s] => Containers %s%n",
                    host.toString(),
                    hostMap.get(host));
            this.containerCount = (hostMap.get(host)).size();
        }

        // endregion

        // region stream segment data

        PrintHelper.format(PrintHelper.Color.BLUE,"%nInformation for Stream %s/%s:%n%n",
                this.conf.scope,this.conf.stream);

        // segments
        PrintHelper.format(PrintHelper.Color.BLUE,"Segments:%n");

        List<SegmentRecord> segmentRecords = zk.getSegmentData();

        for (SegmentRecord segmentRecord:
                segmentRecords) {

            // get the segment records
            Date startTime = new Date(segmentRecord.getStartTime());
            PrintHelper.format("Segment #%s:\tStart Time: %s%n",
                    segmentRecord.getSegmentNumber(),
                    startTime);

            // get the container ID
            SegmentToContainerMapper toContainerMapper = new SegmentToContainerMapper(this.containerCount);

            String streamSegmentName = String.format("%s/%s/%s",conf.scope,conf.stream,segmentRecord.getSegmentNumber());
            int containerId = toContainerMapper.getContainerId(streamSegmentName);

            containerToSegmentNameMap.put(containerId, streamSegmentName);

            PrintHelper.format("\t\tContainer Id: %d%n",containerId);

            Optional<Host> host = zk.getHostForContainer(containerId);
            host.ifPresent(
                    x -> PrintHelper.format("\t\tSegment Store host: [%s]",x)
            );
            PrintHelper.format("%n%n");
        }
        if (conf.txn) {
            if (!zk.getTxnExist()) {
                ExceptionHandler.NO_TXN.apply();
            }

            else{
                Map<String, ActiveTxnRecord> activeTxs = zk.getActiveTxs();
                List<String> completedTxs = zk.getCompleteTxn();

                PrintHelper.println(PrintHelper.Color.PURPLE, "Active transactions: ");
                for (Map.Entry<String, ActiveTxnRecord> activeTx:
                     activeTxs.entrySet()) {
                    PrintHelper.printHead(activeTx.getKey());
                    activeTx.getValue().print();
                }

                PrintHelper.println();
                PrintHelper.println(PrintHelper.Color.PURPLE, "Completed transactions: ");
                for (String completedTx:
                     completedTxs) {
                    byte[] data = zk.getCompleteTxnData(completedTx);
                    CompletedTxnRecord record = CompletedTxnRecord.parse(data);
                    PrintHelper.printHead(completedTx);
                    record.print();

                }


                PrintHelper.println();


            }

        }
        if(conf.bookkeeper) {

            // get the container metadata
            PrintHelper.format(PrintHelper.Color.BLUE,"Container data log metadata: %n");
            for (Integer containerId :
                    containerToSegmentNameMap.keySet()) {
                LogMetadata tmp = zk.getLogMetadata(containerId);
                PrintHelper.format("Container #%d: ", containerId);
                this.logs.add(tmp);

                PrintHelper.println(tmp);
            }
        }

        zk.close();
        return this;
    }

    private StreamStat printHDFSData(){
        PrintHelper.println();
        PrintHelper.println(PrintHelper.Color.PURPLE, "REGION TIER-2 HDFS DATA: ");
        PrintHelper.println();
        try (HDFSHelper hdfsHelper = new HDFSHelper(this.conf.hdfsURL)){
            for (Integer containerId:
                    this.containerToSegmentNameMap.keySet()) {
                String segmentName = containerToSegmentNameMap.get(containerId);

                PrintHelper.println();
                PrintHelper.printHead("Data for segment");
                PrintHelper.print(String.format("%s%n%n",segmentName));

                try {
                    hdfsHelper.printSegmentState(segmentName,segmentNameToIdMap);
                    hdfsHelper.printAll(segmentName, conf.data);
                } catch (HDFSException e) {
                    PrintHelper.processEnd();
                    if (e.isMissingFile){
                        PrintHelper.printError(String.format("No file of the segment %s is in HDFS Storage now", segmentName));
                    } else{
                        PrintHelper.printError("Error: " + e.getMessage());
                    }
                }

            }
        } catch (SerializationException e){
            System.err.println("Error: " + e.getMessage());
            System.exit(-1);
        } catch (IOException e) {
            ExceptionHandler.HDFS_CANNOT_CONNECT.apply();
        }
        return this;
    }

    /**
     * Print data log
     * @return this
     */
    private StreamStat printBKData(){

        // if configure shows not to print T1 log, skip
        if (!conf.bookkeeper)
            return this;
        PrintHelper.println(PrintHelper.Color.PURPLE, "REGION TIER-1 LOGS:");
        ClientConfiguration conf = new ClientConfiguration();

        conf.setZkServers(this.conf.zkURL);
        conf.setZkLedgersRootPath(String.format(ZKHelper.BK_PATH,clusterName));

        // start the bk client
        try(BookKeeper bkClient = new BookKeeper(conf)) {
            for (LogMetadata log:
                 logs) {
                OperationAnalyzer operationAnalyzer = new OperationAnalyzer(bkClient,
                        containerToSegmentNameMap.get(log.getContainerId()), log,
                        segmentNameToIdMap, this.conf);
                PrintHelper.println();
                PrintHelper.printHead("Data for segment");
                PrintHelper.print(String.format("%s%n%n",containerToSegmentNameMap.get(log.getContainerId())));

                operationAnalyzer.printLog();

                // if we cannot found segment id in T1 and Storage
                if (this.conf.simple &&
                        !segmentNameToIdMap.keySet()
                                .contains(containerToSegmentNameMap
                                        .get(log.getContainerId()))){
                    ExceptionHandler.NO_SEGMENT_ID.apply();
                }

            }
        } catch (SerializationException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(-1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return this;
    }

    private StreamStat summarize(){
        ExceptionHandler.summarize();
        return this;
    }
    private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args);
    }

    private static Options getOptions() {
        final Options options = new Options();
        options.addOption("i","input", false, "Input stream name to get status.");
        options.addOption("a","all",false, "Display all of the container logs.");
        options.addOption("s","storage",false,"Only print data in storage.");
        options.addOption("d","data",false,"Display all the data in the stream.");
        options.addOption("c","cluster",false, "Display the cluster information.");
        options.addOption("h","help", false, "Print this help.");
        options.addOption("t","txn",false, "Print transactions info.");
        options.addOption("f", "fence", false, "[WARNING] this option will fence out pravega cluster");
        options.addOption("e","explicit", false, "Wait until get the explicit log");
        options.addOption("l", "log", false, "Display all the logs in tier-1.");
        return options;
    }

    private void end(){
        PrintHelper.println("ByeBye!");
    }



    @Setter
    static class StreamStatConfigure{
        private String scope;
        private String stream;
        private String zkURL;
        private String hdfsURL;

        boolean data = false;
        boolean simple = true;
        boolean bookkeeper = true;
        boolean cluster = false;
        boolean txn = false;
        boolean fence = false;
        boolean explicit = false;
        boolean log = false;
    }

}