# Experiment Automation Scripts

This module contains set of Ansible roles, playbooks and modules for automating the benchmark process.

# How To Deploy a Kafka Cluster on AWS

Following command deploys a Kafka cluster with Zookeepr on AWS.

```
ansible-playbook -i inventory --private-key ~/Documents/imac-keypair.pem kafka.yml
```

# How To Deploy a Hadoop Cluster on AWS

Execute the following command to deploy a Hadoop Cluster on AWS. Make sure to change configurations in ```group_vars/all``` to reflect your needs.

```
ansible-playbook -i inventory --private-key ~/Documents/imac-keypair.pem hadoop.yml
```

## Client Instance Setup

```
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer
sudo apt-get install git
git clone https://github.com/FreshetDMS/FDBench.git
cd FDBench/
git checkout automation
./gradlew :fdbench-tools:tar
cd fdbench-tools/
cd build/distributions/
tar xzf fdbench-tools-0.1-SNAPSHOT-dist.tgz
cd bin/
./run-class.sh org.pathirage.fdbench.tools.SimpleKafkaProducer -b ec2-54-191-194-64.us-west-2.compute.amazonaws.com:9092 -t test1 -r 2000 -d 60
```
