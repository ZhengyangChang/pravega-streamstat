# pravega-streamstat
A Pravega Stream Status Tool

## Usage
```
./gradlew installDist
```

Distributions will be installed in 
```
build/install/pravega-stream-status-tool
```

use
```
bin/StreamStat
```
to run program.

## Instructions
```
usage: bin/StreamStat
 -a,--all        Display all of the container logs.
 -c,--cluster    Display the cluster information.
 -d,--data       Display all the data in the stream.
 -e,--explicit   Wait until get the explicit log.
 -f,--fence      [WARNING] this option will fence out pravega cluster
 -h,--help       Print this help.
 -i,--input      Input stream name to get status.
 -l,--log        Display all the logs in tier-1.
 -s,--storage    Only print data in storage.
 -t,--txn        Print transactions info.
 ```


## Features

This tool will display the stream segments and their data's length and offset range in tier-1 and tier-2 by default.

The default stream is `examples/someStream`
The two inputs are zookeeper url and hdfs url.
You can edit the configure file in `conf/config.properties`
Or copy the pravega cluster configure file to this location.

| **Options** | **Feature**                                                 | 
|-------------|-------------------------------------------------------------|
| **-a**      | Display all the logs in container, should be used with -l.  |
| **-c**      | Display the information of the pravega cluster.             |
| **-d**      | In both tier, print out the data stored. Otherwise program will only print datalength.|
| **-e**      | Since some log might be locked by the pravega cluster, it may take some time to wait until the cluster release the log, program won't wait by default, but to get the explicit log, you should use this option to wait until the explicit log is read.|
| **-f**      | Dangerous! This option is only used on an offline cluster, this will fence out the cluster's writing to get explicit log. Using this on a living cluster will cause unexpected effect on cluster.|
| **-h**      | Display help message.           |
| **-i**      | Input the stream you want to inspect. You can also set stream in config.properties |
| **-l**      | Display the logs related to given stream. |
| **-s**      | Only show the status in tier-2 storage. |
| **-t**      | Display all the transactions related to given stream. |