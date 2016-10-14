Husky on Yarn
=============

[![Build Status](https://travis-ci.org/husky-team/husky-on-yarn.svg?branch=master)](https://travis-ci.org/husky-team/husky-on-yarn)

Make Husky run on [YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html).


Dependencies
-------------

* Java 1.7+
* Maven 3.3.9
* Hadoop 2.6+

Usage
-------------

One can use `run.sh` to run a husky application on yarn:
```shell
run.sh /path/to/master /path/to/app /path/to/config
```
This script will first build this project and submit a job to yarn.

Before running `run.sh`, please ensure that you've built husky master and application (see [details](https://github.com/husky-team/husky/blob/master/README.md)), and prepared a nice config file. `master_port` and `comm_port` must be provided by the config file. See an example below:

```txt
# Required
master_port=yyyyy
comm_port=yyyyy

# Optional
hdfs_namenode=xxx.xxx.xxx.xxx
hdfs_namenode_port=yyyyy
```

By default, `run.sh` creates two one worker node, which contains two workers, on **localhost** to run the application. This can be changed by modifying `--worker.info` in `run.sh`.

Husky-on-yarn supports more arguments for different purposes. Get more information by running `run.sh` without giving any arguments:
```
 -app_name <arg>           The name of the application
 -app_priority <arg>       A number to indicate the priority of the husky
                           application
 -application <arg>        Executable for c++ husky worker (on local file
                           system or HDFS)
 -config <arg>             Configuration file for c++ husky master and
                           application (on local file system or HDFS)
 -container_memory <arg>   Amount of memory in MB to be requested to run
                           container. Each container is a worker node.
 -container_vcores <arg>   Number of virtual cores that a container can
                           use
 -help                     Print Usage
 -jar <arg>                Local path to the jar file of application
                           master.
 -key_tab <arg>            Path on DATANODEs to keytab file which is
                           required by kinit to avoid security issues of
                           HDFS
 -ld_library_path <arg>    Path on datanodes where c++ husky master and
                           application looks for their libraries
 -local_archives <arg>     Archives that need to pass to and be unarchived
                           in working environment. Use comma(,) to split
                           different archives.
 -local_files <arg>        Files that need to pass to working environment.
                           Use comma(,) to split different files.
 -local_resrcrt <arg>      The root directory on hdfs where local
                           resources store
 -log_to_hdfs <arg>        Path on HDFS where to upload logs of
                           application master and worker containers
 -master <arg>             Executable for c++ husky master (on local file
                           system or HDFS)
 -master_memory <arg>      Amount of memory in MB to be requested to run
                           application master
 -user_name <arg>          Username used for kinit
 -worker_infos <arg>       Specified hosts that husky application will run
                           on. Use comma(,) to split different archives.
```

Contributors
---------------

* zzxx (yjzhao@cse.cuhk.edu.hk, zzxx.is.me@gmail.com)
* legend (gxjiang@cse.cuhk.edu.hk, kygx.legend@gmail.com)


License
---------------

Copyright 2016 Husky Team

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
