# Run Hadoop Cluster within Docker

![hadoop-cluster-docker](hadoop-cluster-docker.png)

## 3 Nodes (1 master and 2 workers) Hadoop Cluster

### 1. Clone Repository

```bash
git clone https://github.com/cihatislamdede/bigdata-hadoop-cluster
```

### 2. Start containers

```bash
sudo chmod +x start.sh
./start.sh
```

```bash
start hadoop-master container...
start hadoop-slave1 container...
start hadoop-slave2 container...
root@hadoop-master:~# 
```

### 3. Start hadoop cluster

```bash
./start-hadoop.sh
```

### 4. Run wordcount job to test hadoop cluster

```bash
./run-wordcount.sh
```

```bash
input file1.txt:
Hello Hadoop

input file2.txt:
Hello Docker

wordcount output:
Docker    1
Hadoop    1
Hello    2
```

### 5. Some useful commands

- copy source code to hadoop cluster

```bash
docker cp BigData/ hadoop-master:/root
```

- docker first run and average job run

```bash
cp book.txt input/
cd BigData
mvn package
cd ..
cp BigData/target/BigData-1.0.jar input/
hdfs dfs -put -f input/BigData-1.0.jar input/ 
hdfs dfs -put -f input/book.txt input/
# Usage: <input> <output> <reducer> <key_idx> <value_idx>
hadoop jar input/BigData-1.0.jar org.example.Main input/book.txt output-avg avg 10 3
hdfs dfs -cat output-avg/*
```

### 6. Clear all environment

```bash
./clear.sh
```
