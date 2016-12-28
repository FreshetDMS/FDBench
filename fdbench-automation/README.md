# Experiment Automation Scripts

This module contains set of Ansible roles, playbooks and modules for automating the benchmark process.

# Requirements

* Create EC2 instances if needed
* Deploy Kafka based on experiment configuration
* Initialize the experiment
* Run necessary tasks or jobs related to experiment based on configuration
* Collect metrics and results
* Upload them to somewhere based on experiment configuration

## Details

* We need to spawn Kafka clusters with different configurations (Different IOPS, CPUs, number of disks, default partitions, replication factors)
* Then we need to run different workloads and collect metrics
* Workload is a YARN app
* So we need a YARN deployment too

# TODO

* Improve makefs role (*Multiple disk support is not necessary at this stage*)
* Add cluster shutdown
* Add topic creation and data loading
* We need to write a tool to create/delete Kafka topics and some data generators
* Investigate YARN deployment (May be call ec2-hadoop script from Ansible)
* Change makefs role to use loop_control and iterate over possible disks

## Immediate TODO

* Wrap yarn-ec2 as an Ansible library
* Deploy latest Kafka on EC2
* Run some experiments to figure out things needed for the paper