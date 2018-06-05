# Map Reduce Framework

A MapReduce Framework for a single computer, which is done by creating a single thread for each task. And the goal of this framework is to optimize the processing time of a program by taking advantage of the available cores in our multi-core computer.

## Design Remarks

All information about the methods in the classes, explained extensively as
comments in the .cpp and .h.

__Map workers__

We decided to implement a map that will store for every thread
its id and its corresponding container of <key2,value2>.

Each thread can access his container (in order, for example, add elements of
<key2,value2>) via [pthread_self()].

To avoid scenarios in which the shuffler (Its design and implementation will be
depicted later on) tries to access thread's container while this thread
works on that container, we chose to guard every container with a unique mutex.
(A mutex for every container instead of one mutex in order to increase the
concurrency).

__Mapping is done as followed__

The input list (pairs of <key1,value1>) is iterated from the last index to the
first, and each thread is given up to CHUNK_OF_TASKS pairs to process
(send to map() and then to emit2()).

__Shuffler__

Shuffler's goal is to organize all pairs of <key2,value2> to a container of
<key2,container<value2>>.
We chose to implement a vector of tasks for the shuffle.
Every map worker is responsible to add a task and request shufller's service
(via signaling the shuffler that a task is ready for him to perform).
Single task contains- i) thread's id - shuffler will READ pairs from a container
that coresponed to this id. ii) indices of <k2,v2> pairs in this container
that are ready to be processed. [This indices are calculated by the thread
itself- starting index is its container size before adding pairs of
<key1,value1> by emit2, and ending index is the size (minus 1) of that container
after mapping CHUNK_OF_TASKS pairs of <key1,value1> (mapping = eventually
producing pairs of <key2,value2>)].
Shuffler updates ShuffledTasks which is the desired <key2,container<value2>>
container. (Its a map. every key is key2, and the value is its corresponding
container<value2>.

__Reduce workers__

Exactly as the map workers (exept allocating each worker CHUNK_OF_TASKS pairs of
<key1,value1> from the input lists, each worker will process up to
CHUNK_OF_TASKS pairs of <key2,container<value2>> from ShuffledTasks).
