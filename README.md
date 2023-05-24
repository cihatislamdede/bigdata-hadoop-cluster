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

#### copy file to hadoop cluster

```bash
docker cp BigData/ hadoop-master:/root
docker cp 18data.txt hadoop-master:/root
```

#### docker first run

```bash
cp 18data.txt input/
cd BigData
mvn package
cd ..
cp BigData/target/BigData-1.0-SNAPSHOT.jar input/
hdfs dfs -put -f BigData/target/BigData-1.0-SNAPSHOT.jar input/
hdfs dfs -put -f input/18data.txt input/
yarn jar input/BigData-1.0-SNAPSHOT.jar org.example.Summation input/18data.txt output
hdfs dfs -cat output/*
```

#### docker re-run

```bash
rm -r BigData/target/
cd BigData
mvn package
cd ..
cp BigData/target/BigData-1.0-SNAPSHOT.jar input/
hdfs dfs -put -f BigData/target/BigData-1.0-SNAPSHOT.jar input/
yarn jar input/BigData-1.0-SNAPSHOT.jar org.example.Summation input/18data.txt output
hdfs dfs -cat output/*
```

### 6. Clear all environment

```bash
./clear.sh
```
