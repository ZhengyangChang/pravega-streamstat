package service;

import bookkeepertool.LogMetadata;
import io.pravega.common.cluster.Host;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import record.ActiveTxnRecord;
import record.SegmentRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


/**
 * Zookeeper helper functions
 */
class ZKHelper {

    // region constants
    private static final String CONTROLLER_PATH = "/cluster/controllers";
    private static final String HOST_MAP_PATH = "/cluster/segmentContainerHostMapping";
    private static final String STREAM_PATH = "/store/%s/%s";
    private static final String SEGMENT_PATH = STREAM_PATH + "/segment";
    private static final String LOG_METADATA_BASE = "/segmentstore/containers";
    private static final int DEFAULT_DEPTH = 2;
    private static final int DIVISOR = 10;
    private static final String SEPARATOR = "/";
    private static final String TXN_PATH = "/transactions";
    private static final String ACTIVE_TX_ROOT = TXN_PATH + "/activeTx";
    private static final String ACTIVE_TX_PATH = ACTIVE_TX_ROOT + "/%s/%s";
    private static final String COMPLETE_TX_ROOT = TXN_PATH + "/completedTx";
    private static final String COMPLETE_TX_PATH = COMPLETE_TX_ROOT + "/%s/%s";

    static final String BK_PATH = "/pravega/%s/bookkeeper/ledgers";
    // endregion

    // region members

    private CuratorFramework zkClient;
    private String segmentPath;
    private String streamPath;
    private String clusterName;
    private String activeTxPath;
    private String completeTxPath;
    // endregion

    // region constructor

    private ZKHelper(String zkURL,String scope, String stream){
        segmentPath = String.format(SEGMENT_PATH,scope,stream);
        streamPath = String.format(STREAM_PATH,scope,stream);
        activeTxPath = String.format(ACTIVE_TX_PATH,scope, stream);
        completeTxPath = String.format(COMPLETE_TX_PATH,scope, stream);
        createZKClient(zkURL);
        checkStreamExist();
    }

    // endregion
    String getClusterName(){
        return clusterName;
    }

    List<String> getControllers(){
        return getChild(CONTROLLER_PATH);
    }

    List<SegmentRecord> getSegmentData(){
        byte[] data = getData(segmentPath);

        List<SegmentRecord> ret = new ArrayList<>();
        for (int i = 0; i < data.length ; i += 28)
            ret.add(SegmentRecord.parse(data, i));
        return ret;
    }

    Optional<Host> getHostForContainer(int containerId) {

        Map<Host, Set<Integer>> mapping = getCurrentHostMap();
        if (mapping == null)
            return null;
        else
            return mapping
                .entrySet()
                .stream()
                .filter(x -> x.getValue()
                        .contains(containerId))
                .map(Map.Entry::getKey)
                .findAny();

    }

    boolean getTxnExist(){
        return getChild("/").contains(TXN_PATH.replace("/",""));
    }

    private List<String> getActiveTxnSegments(){
        return getChild(activeTxPath);
    }

    Map<String, ActiveTxnRecord> getActiveTxs(){
        Map<String, ActiveTxnRecord> ret = new HashMap<>();

        List<String> segments = getActiveTxnSegments();
        for (String segment:
             segments) {
            List<String> txs = getChild(String.format("%s/%s",activeTxPath,segment));

            for (String tx:
                 txs) {
                byte[] data = getData(String.format("%s/%s/%s", activeTxPath, segment, tx));
                ActiveTxnRecord record = ActiveTxnRecord.parse(data, Integer.parseInt(segment));

                ret.put(tx,record);
            }
        }
        return ret;

    }

    List<String> getCompleteTxn(){
        try {
            return zkClient.getChildren().forPath(completeTxPath);
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    byte[] getCompleteTxnData(String completeTxUUID){
        return getData(String.format("%s/%s",completeTxPath,completeTxUUID));
    }

    Map<Host, Set<Integer>> getCurrentHostMap() {
        Map tmp = (Map) SerializationUtils.deserialize(getData(HOST_MAP_PATH));
        Map<Host, Set<Integer>> ret = new HashMap<>();
        for (Object key:
                tmp.keySet()) {
            if(key instanceof  Host && tmp.get(key) instanceof Set){
                Set<Integer> tmpSet = new HashSet<>();
                for (Object i:
                        (Set) tmp.get(key)) {
                    if(i instanceof Integer){
                        tmpSet.add((Integer) i);
                    }
                }
                ret.put((Host) key,tmpSet);
            }
        }
        return ret;
    }

    LogMetadata getLogMetadata(Integer containerId){
        byte[] data = getData(LOG_METADATA_BASE + getHierarchyPathFromId(containerId));
        return LogMetadata.deserializeLogMetadata(data, containerId);
    }

    static ZKHelper create(String zkURL,String scope, String stream){
        return new ZKHelper(zkURL,scope,stream);
    }

    private List<String> getChild(String path){
        List<String> ret = null;
        try {
            ret = zkClient.getChildren().forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    private byte[] getData(String path){
        byte[] ret = null;
        try {
            ret = zkClient.getData().forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    private String getClusterNameFromZK(){
        List<String> clusterNames = null;
        try {
            clusterNames = zkClient.getChildren().forPath("/");
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (clusterNames == null || clusterNames.size() != 1){
            ExceptionHandler.NO_CLUSTER.apply();
        }

        return clusterNames.get(0);
    }

    private String getHierarchyPathFromId(Integer nodeId){
        StringBuilder pathBuilder = new StringBuilder();
        int value = nodeId;
        for (int i = 0; i < DEFAULT_DEPTH; i++) {
            int r = value % DIVISOR;
            value = value / DIVISOR;
            pathBuilder.append(SEPARATOR).append(r);
        }

        return pathBuilder.append(SEPARATOR).append(nodeId).toString();
    }

    private void createZKClient(String zkURL){
        zkClient = CuratorFrameworkFactory
                .builder()
                .connectString(zkURL)
                .namespace("pravega")
                .retryPolicy(new ExponentialBackoffRetry(2000, 2))
                .build();

        startZKClient();

        clusterName = getClusterNameFromZK();

        zkClient.close();

        zkClient = CuratorFrameworkFactory
                .builder()
                .connectString(zkURL)
                .namespace("pravega/" + clusterName)
                .retryPolicy(new ExponentialBackoffRetry(2000, 2))
                .build();

        startZKClient();
    }

    private void startZKClient(){
        zkClient.start();
        try {
            zkClient.blockUntilConnected();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private void checkStreamExist(){
        try{

            zkClient.getChildren().forPath(streamPath);

        } catch (Exception e) {
            if (e instanceof KeeperException.NoNodeException) {
                ExceptionHandler.NO_STREAM.apply();
            }
        }
    }

    void close(){
        zkClient.close();
    }

}
