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
*** you will see the following output:***
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
***output***
input file1.txt:
Hello Hadoop

input file2.txt:
Hello Docker

wordcount output:
Docker    1
Hadoop    1
Hello    2
```

### 5. Clear all environment

```bash
./clear.sh
```
