# FDBench

FDBench is a fast data management benchmark.

# Fast Data

> Big data is often created by data that is generated at incredible speeds, such as click-stream data, financial ticker data, log aggregation, or sensor data. Often these events occur thousands to tens of thousands of times per second. No wonder this type of data is commonly referred to as a "fire hose."

> When we talk about fire hoses in big data, we're not measuring volume in the typical gigabytes, terabytes, and petabytes familiar to data warehouses. We're measuring volume in terms of time: the number of megabytes per second, gigabytes per hour, or terabytes per day. We're talking about velocity as well as volume, which gets at the core of the difference between big data and the data warehouse. Big data isn't just big; it's also fast.

*From [Fast data: The next step after big data](http://www.infoworld.com/article/2608040/big-data/fast-data--the-next-step-after-big-data.html)*

# Architecture

FDBench's architecture is similar to Apache Samza's architecture. Each benchmark is a collection of tasks that perform same or different tasks based on the benchmark. For example, *Kafka producer throughput benchmark's* tasks do the same thing -- publishing messages to different partitions in a Kafka topic where as benchmark which measures the Kafka's behavior in a real environment will be a collection of producer tasks and consumer tasks.

Current implementation uses Apache YARN to implement the benchmark job abstraction.


# Benchmarks

## Messaging Bench

Distributed data generator and performance measurement tool for messaging middleware such as Apache Kafka. 

**Please note that this is still under development.**
