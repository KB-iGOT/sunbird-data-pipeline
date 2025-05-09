# Data-pipeline troubleshooting

To verify if data-pipeline is working properly
- Ensure that if a unique, valid, recent event is ingested from kafka ingestion topic (e.g. `dev.telemetry.ingest`), it
  is reaching the topic druid ingests from (e.g. `dev.druid.events.telemetry`)
    - get golden data set from `test-data/flink_golden_dataset.json`
    - update `data.params.msgid` to a unique uuid
    - for each event in `data.events`, update `mid` to a unique uuid
    - update time fields to a recent time, because `druid-event-validator` job drops older events (3 months) silently
- When the event is confirmed to be reaching druid ingestion topic, check if a data-source is created in Druid, and the
  message is added

## Kafka

```sh
# ssh to kafka server (KP)
cd opt/kafka/bin

# to create a topic
./kafka-topics.sh --create --topic <topic> --replication-factor 1 --partitions 1 --bootstrap-server localhost:9092

# to create topic for older version of kafka
./kafka-topics.sh --create --topic <topic> --replication-factor 1 --partitions 1 --zookeeper localhost:2181

# to list topics
./kafka-topics.sh --list --zookeeper localhost:2181

# start consumer add `--from-beginning` to get all messages from start
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <topic>

# to start a producer
./kafka-console-producer.sh --topic <topic> --broker-list localhost:9092

```

## Flink jobs

### check logs for flink jobs
```sh
# ssh to kubernetes server (jenkins in our case)
# export Kubernetes config file environment variable
export KUBECONFIG=/path/to/kube-config.yaml

# list pods flink-dev namespace
kubectl get po -n flink-dev

# get logs for a pod
kubectl logs <pod> -n flink-dev
```

if logs contain errors because of missing topics
check if config for flink jobs is correct (`ansible/kubernetes/helm_charts/datapipeline/flink-jobs/values.j2`)
or, to create missing topics, ssh to KP(kafka) server and create the topic
```sh
# ssh to kafka server (KP)
cd opt/kafka/bin
./kafka-topics.sh --create --topic <topic> --replication-factor 1 --partitions 1 --bootstrap-server localhost:9092
# for older version of kafka
./kafka-topics.sh --create --topic <topic> --replication-factor 1 --partitions 1 --zookeeper localhost:2181
```

### flink state backend
if flink is unable to connect to state backend, check your state-backend config
or, optionally you can turn off the state backend (flink will store state in memory, but will not be able to recover state in case pod crashed)
```sh

```

Note: for more detailed info on different kafka topics and flink jobs click [here](https://github.com/shobhit-vashistha/sb-dp-doc#readme "Data-pipeline doc")

## Druid

### Change log level for druid services

to change log level for any of the druid services edit their respective `log4j2.xml` file.
for example to set broker log level to warn, edit it's `log4j2.xml` -

Note: setting loglevel above `WARN` (e.g. `INFO`) will make logs very busy, and log files would inflate to
MBs in a couple of minutes, set loglevel back to `ERROR` as soon as done with debugging

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Configuration status="WARN">
  <Appenders>
    <RollingFile name="File" fileName="/var/log/druid//broker.log" filePattern="/var/log/druid//broker.%i.log">
      <PatternLayout>
        <Pattern>"%d{ISO8601} %p [%t] %c - %m%n"</Pattern>
      </PatternLayout>
      <Policies>
        <SizeBasedTriggeringPolicy size="50 MB"/>
      </Policies>
      <DefaultRolloverStrategy max="20"/>
    </RollingFile>
  </Appenders>
  <Loggers>
    <Root level="warn">
      <AppenderRef ref="File"/>
    </Root>
  </Loggers>
</Configuration>
```

### Druid config

common druid config for services is present at `/data/druid/conf/druid/_common/common.runtime.properties`
config for individual services is present at `/data/druid/conf/druid/<service e.g. broker or overlord>/runtime.properties`

#### for s3 compatible deep storage

to use s3 as deep storage make sure `common.runtime.properties` contains following config

```
druid.extensions.loadList=["druid-s3-extensions"]
druid.extensions.directory=/data/druid/extensions

druid.storage.type=s3
druid.storage.bucket=<bucket>
druid.storage.baseKey=druid/segments

druid.s3.accessKey=<access_key>
druid.s3.secretKey=<secret_key>

# set protocol and endpoint together
druid.s3.endpoint.url=<prototocol>://<host>

# or separately as
# druid.s3.endpoint.url=<host>
# druid.s3.endpoint.protocol=<prototocol>
```

for non-aws s3-like stores (like ceph), we might have to add additional config
```
# enable access of bucket from any region
druid.s3.forceGlobalBucketAccessEnabled=true

# to enable path like access
# if true,  url=<protocol>://<host>/<bucket> 
# if false, url=<protocol>://<bucket>.<host> 
druid.s3.enablePathStyleAccess=true
```

to allow Druid to publish task logs to s3 add following config
```
druid.indexer.logs.type=s3
druid.indexer.logs.s3Bucket=<bucket>
# path to logs within the bucker
druid.indexer.logs.s3Prefix=druid/stage/indexing_logs
```

additional config for s3 deep storage (optional)
```
# uncomment to enable server side encryption for s3
# druid.storage.sse.type=s3

# uncomment to enable v4 signing of requests
# druid.s3.endpoint.signingRegion=<aws-region-code>

# uncomment to disable chunk encoding
# druid.s3.disableChunkedEncoding=true
```

#### S3 bucket policy

Druid should have permissions to read and write from `druid` dir of the bucket
For S3, we would require `GetObject`, `PutObject`, `GetObjectAcl`, `PutObjectAcl` permissions

Example policy might look like-

`policy.json`
```json
{
  "Statement": [
    {
      "Action": [
        "s3:ListAllMyBuckets"
      ],
      "Effect": "Allow",
      "Resource": "arn:aws:s3:::*"
    },
    {
      "Action": [
        "s3:ListBucket",
        "s3:ListBucketMultipartUploads",
        "s3:GetBucketLocation",
        "s3:AbortMultipartUpload",
        "s3:GetObjectAcl",
        "s3:GetObjectVersion",
        "s3:DeleteObject",
        "s3:DeleteObjectVersion",
        "s3:GetObject",
        "s3:PutObjectAcl",
        "s3:PutObject",
        "s3:GetObjectVersionAcl"
      ],
      "Effect": "Allow",
      "Resource": [
          "arn:aws:s3:::<bucket>/druid",
          "arn:aws:s3:::<bucket>/druid/*"
      ]
    }
  ]
}
```

to update bucket policy using `s3cmd`, first install s3cmd and configure using `s3cmd --configure`, then run

```sh
s3cmd setpolicy policy.json s3://<bucket>
```

Depending on the library being used to upload files to storage, to make files accessible publicly we might need to set acl to public using `s3cmd`

```sh
s3cmd setacl s3://<bucket>/<folder>/* --acl-public
```

#### for azure deep storage

to use azure as deep storage make sure `common.runtime.properties` contains following config

```
druid.extensions.loadList=["druid-azure-extensions"]
druid.extensions.directory=/data/druid/extensions

druid.storage.type=azure
druid.azure.account=<account>
druid.azure.key=<key>
druid.azure.container=<container>
```

to allow Druid to publish task logs to azure add following config
```
druid.indexer.logs.type=azure
druid.indexer.logs.container=<container>
druid.indexer.logs.prefix=<prefix e.g. druidlogs>
```

#### misc config
```
# uncomment to disable acl for deep storage
# druid.storage.disableAcl=true

# uncomment to disable acl for only logs
# druid.indexer.logs.disableAcl=true
```

### Druid graceful restart / rolling update

For configurations to take effect Druid services for which config has changed must be restarted.
All Druid services except for `middlemanager` can be restarted safely through `systemctl`

```sh
# ssh to druid
systemctl restart druid_broker.service
systemctl restart druid_coordinator.service
systemctl restart druid_historical.service
systemctl restart druid_overlord.service
```

to gracefully restart `middlemanager` first we have to suspend all running supervisors. this publishes
segments which have not been published yet

```sh
# ssh to druid
# get running supervisor names
curl -X GET http://localhost:8081/druid/indexer/v1/supervisor -i

# do this for all running supervisors
# suspend supervisor (stop running tasks and publish segments)
curl -X POST http://localhost:8090/druid/indexer/v1/supervisor/<supervisor-name>/suspend

# restart middlemanager service
systemctl restart druid_middlemanager.service

# resume suspended supervisors
curl -X POST http://localhost:8090/druid/indexer/v1/supervisor/<supervisor-name>/resume
```

### Druid API

ports - to find out what ports each of the services are running check `runtime.properties` file in `/data/druid/conf/druid/<service>/`

default ports -
```sh
# coordinator - 8081
# broker - 8082
# historical - 8083
# overlord - 8090
# middlemanager - 8091
```

Check status, get data sources
```sh
# check status of overlord service
curl -X GET http://localhost:8090/status

# show data sources
curl -X GET http://localhost:8081/druid/coordinator/v1/datasources -i
```

Manage Ingestion
```sh
# get running supervisor names
curl -X GET http://localhost:8081/druid/indexer/v1/supervisor -i

# inspect particular supervisor ingestion config
curl -X GET http://localhost:8081/druid/indexer/v1/supervisor/<supervisor-name> -i

# inspect particular supervisor status
curl -X GET http://localhost:8081/druid/indexer/v1/supervisor/<supervisor-name>/status -i

# inspect particular supervisor task stats
curl -X GET http://localhost:8081/druid/indexer/v1/supervisor/<supervisor-name>/stats -i

# inspect tasks
curl -X GET http://localhost:8081/druid/indexer/v1/supervisor/tasks -i

# inspect pending tasks
curl -X GET http://localhost:8081/druid/indexer/v1/supervisor/pendingTasks -i

# inspect running tasks
curl -X GET http://localhost:8081/druid/indexer/v1/supervisor/runningTasks -i


# add new supervisor
curl -X POST -H 'Content-Type: application/json' -d @spec.json http://localhost:8090/druid/indexer/v1/supervisor

# stop and delete supervisor
curl -X POST http://localhost:8090/druid/indexer/v1/supervisor/<supervisor-name>/terminate -i

# suspend supervisor (stop running tasks and publish segments)
curl -X POST http://localhost:8090/druid/indexer/v1/supervisor/<supervisor-name>/suspend

# resume supervisor
curl -X POST http://localhost:8090/druid/indexer/v1/supervisor/<supervisor-name>/resume

```