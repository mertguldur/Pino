# Pino

### Motivation

Pino is a distributed key-value storage system with fault tolerance and load balancing. It is built for searching keywords in big datasets efficiently, namely, Pino provides constant time performance in returning all the values associated with the given key.   

### Features and Functions

1. It is based on insertion, lookup and deletion of key-value pairs where each key can have multiple values. It supports concurrent insertions, lookups and deletions.

2. The current implementation uses the full IMDb dataset having ~2.4 million movies, and a keyword search (lookup) which returns all the movie titles including the keyword takes several milliseconds.

3. It is distributed since a single computer becomes performance bottleneck, especially in concurrent operations, and consumes too much memory to accomodate efficient searches.

4. It provides load balancing; each computer is responsible for equally sized ranges of the dataset. Insertion of key-value pairs are also automatically routed to the computer with the lowest storage load.

5. It is fault tolerant; the system can deal with any number of simultaneous computer failures which can be provided as a parameter while launching the program. After recovery, failed computers are allowed to rejoin the system. The system doesn't lose any data during failures and rejoins but the more failures it is ordered to tolerate, the more replicated data it stores, therefore, it consumes more memory. In case of failures and rejoins it reconstructs its network overlay and continues to function properly.

7. It is scalable; all of its features are designed to function in systems with arbitrary number of computers.

8. The dataset file is read and replicas are created in parallel so as to speedup the storage initialization in computers with multi-core processors.

### Running Instructions

Inside the bin folder, run the following command:

<pre><code>java Launcher &lt;port> &lt;datasetFileName> &lt;maximumNumberOfFailuresToTolerate>
</code></pre>

The data set file should be put inside the Pino directory.  

It is also recommended to increase the Java heap space with the following flag when using a big dataset.

<pre><code>-Xmx&lt;size>
</code></pre>