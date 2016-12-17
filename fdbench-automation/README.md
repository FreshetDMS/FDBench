# Experiment Automation Scripts

This module contains set of Ansible roles, playbooks and modules for automating the benchmark process.

# Requirements

* Create EC2 instances if needed
* Deploy Kafka based on experiment configuration
* Initialize the experiment
* Run necessary tasks or jobs related to experiment based on configuration
* Collect metrics and results
* Upload them to somewhere based on experiment configuration

# TODO

* Improve makefs role (*Multiple disk support is not necessary at this stage*)
* Add cluster shutdown
* Add topic creation and data loading
* We need to write a tool to create/delete Kafka topics and some data generators
* Investigate YARN deployment (May be call ec2-hadoop script from Ansible)