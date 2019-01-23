# RCache

Cache files recursively on HDFS.

usage: `hadoop jar RCache.jar -d <Dir> -p <Pool> [-r <Replication=1>] [-v <Verbose>] [-c <Path>] [-show <show=false>] [-h <Help>]`

| Parameter | Description |
|---|---|
| -d,--Directory <arg>   |   Root path which would be cached.   |
| -h,--help              |   Get help.                          |
| -p,--Pool <arg>        |   Cache pool name.                   |
| -r,--Replication <arg> |   Amount of cache replication.       |
  |-c,--check <arg>|Check directives whether fully cached or not.|
  |-show,--show|This option will print all fully cached directives.|
| -v,--Verbose           |   Verbosely list files processed.    |

If it can help you, start it. :)
