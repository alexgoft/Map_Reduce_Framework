#include "MapReduceFramework.h"
#include <vector>
#include <map>
#include <iostream>
#include <ctime>
#include <ratio>
#include <chrono>
#include <queue>
#include <fstream>
#include <iomanip>

//---------------------------------Macros-------------------------------------//

#define SUCCESS 0
#define FAILURE -1
#define LAST_INDEX 1
#define OFFSET 1
#define CHUNK_OF_TASKS 10
#define CONT_WAIT_TIME 1000000
#define CONT_WAIT_TO_SEC 1000000000

//--------------------------------typedefs------------------------------------//

// For map
typedef std::vector<IN_ITEM> IN_ITEMS_VECTOR;
typedef std::pair<k2Base*,v2Base*> MAP_CONTAINER_ITEM;
typedef std::vector<MAP_CONTAINER_ITEM> MAP_THREAD_CONTAINER;

// For Reduce
typedef std::vector<OUT_ITEM> OUT_ITEMS_VECTOR;

// For shuffle
typedef std::pair<pthread_t, std::pair<int,int>> SHUFFLER_TASK;

//-----------------------------Global Variables-------------------------------//

// Type of workers.
typedef enum state {MAPPER,REDUCER,SHUFFLER} WorkerType;

// The array of id's of worker threads.
pthread_t * threads;
// Shuffler's thread id.
pthread_t shuffler;

// Number of threads
int poolSize;

// The MapReduceBase instance
MapReduceBase *functions;

// Number of working map threads threads that finished
int mapThreadsFinished;

// This mutex guards mapThreadsFinished
pthread_mutex_t mapThreadsFinishedMutex;

// A mutex that guards mapThreadContainerMap and reduceThreadContainerMap.
pthread_mutex_t threadContainerMutex;

//-------------------------------Map Logic------------------------------------//

// Map that associates thread id and its container
std::map<pthread_t , MAP_THREAD_CONTAINER> mapThreadContainerMap;

// Map that associates thread id and its mutex thats guards the container.
std::map<pthread_t , pthread_mutex_t> mapThreadMutexMap;

// The vector of the input values for the maps
IN_ITEMS_VECTOR itemsToMapVector;

// Will be used to guard the index of itemsToMapVector.
pthread_mutex_t mapIndexMutex;

// Current index of input pair.
int itemsToMapVectorIndex;

//--------------------------Shuffler Logic------------------------------------//

/**
 * A custom comperator used in the shuffledTasks map. Its main goal is to
 * probide ability to compare between k2Base objects.
 */
struct customCmp {
    bool operator()(const k2Base* a, const k2Base* b) const
    {
        return *a<*b;
    }
};

// A map that for each key2 there's a vector of value2.
std::map<k2Base*, V2_LIST, customCmp> shuffledTasks;

// Shufflers conditional variable.
pthread_cond_t shufflerCV;

// Will be used to guard the index of itemsToMapVector.
pthread_mutex_t shufflerMutex;

// Vector of threads that signaled the shuffler to process their data.
std::vector<SHUFFLER_TASK> shufflerTasks;

//--------------------------Reduce Logic------------------------------------//

// Map that associates thread id and its container (vector) of <k3,v3>
std::map<pthread_t , OUT_ITEMS_VECTOR> reduceThreadContainerMap;

// Will be used to guard the index of itemsToMapVector.
pthread_mutex_t reduceIndexMutex;

// global iterator that points to the cell that all cells to it were proccessed
std::map<k2Base*, V2_LIST, customCmp>::iterator mainReduceIterator;

//----------------------------Final output------------------------------------//

// The final list of <k3,v3> that will be returned sorted.
OUT_ITEMS_LIST outputList;

//---------------------------Time and log file--------------------------------//

// Measure of years starts, by definition, from 1900.
#define START_YEAR 1900
// value to initialize timer funtion
#define INIT 0
// two digit number. (will be filled with 0 if its single digit number)
#define TWO_DIG 2
// If number is not 2-digit number, the gap filled with 0
#define FILL  '0'
// Measure of moth, in this method, has offset of 1 month.
#define MONTH 1

// Thread's going to be created.
#define CREATE "created"
// Thread's going to be terminated.
#define TERMINATE "terminated"

// the file to which we'll write essential data from this framework.
std::fstream logFile;
// Guards the time stamp printing action.
pthread_mutex_t logMutex;

// Variables with which time stamp is measured.
time_t now;
tm *ltm;

/*
 * This method receive a type of a thread and its action, and prints
 * the current time with additional information.
 */
void timeStamp(WorkerType worker, std::string action) {

    std::string type;

    pthread_mutex_lock(&logMutex);

    if(worker == MAPPER)
    {
        type = "ExecMap";
    }
    else if(worker == REDUCER)
    {
        type = "ExecReduce";
    }
    else if (worker == SHUFFLER)
    {
        type = "Shuffle";
    }

    // Time stamp;
    now = time(INIT);
    ltm = localtime(&now);

    logFile << "Thread " << type << " " << action << " [" \
            << std::setw(TWO_DIG) << std::setfill(FILL) << ltm->tm_mday << "." \
            << std::setw(TWO_DIG) << std::setfill(FILL) << MONTH + ltm->tm_mon \
            << '.' << START_YEAR + ltm->tm_year         << ' ' \
            << std::setw(TWO_DIG) << std::setfill(FILL) << ltm->tm_hour << ':' \
            << std::setw(TWO_DIG) << std::setfill(FILL) << ltm->tm_min  << ':' \
            << std::setw(TWO_DIG) << std::setfill(FILL) << ltm->tm_sec  \
            << "]\n";

    pthread_mutex_unlock(&logMutex);
}

//------------------------------Error handle----------------------------------//

// Name of functions that call failureCheckerPrinter().
#define  THREAD_CREATION  "pthread_create()"
#define THREAD_JOIN "pthread_join()"
#define COND_SIGNAL "pthread_cond_signal()"
#define COND_T_WAIT "pthread_cont_timedwait()"
#define COND_DESTROY "pthread_cond_destroy()"
#define  FILE_OPEN "open()"
#define MEMORY_ALOC  "new[]"
#define CLOCK "clock_gettime()"
#define MUTEX_LOCK "pthread_mutex_lock()"
#define MUTEX_UNLOCK "pthread_mutex_unlock()"
#define MUTEX_DESTROY "pthread_mutex_destroy()"

/*
 * Method that receives a function name that called it and a return value of
 * that function, and checks whether is succeeded. If not, a message regarding
 * the failure will be printed and the program will be "shut down".
 */
void failureCheckerPrinter(std::string function, int returnVal)
{
    if(returnVal != SUCCESS)
    {
        std::cerr << "MapReduceFramework Failure: " \
                  << function << " failed." << std::endl;

        // Free memory.
        delete[] threads;

        exit(EXIT_FAILURE);
    }
}

//------------------------Mutex and Error CondVar management------------------//

/*
 * Safely lock a mutex - check return value and operate accordingly using the
 * method failureCheckerPrinter.
 */
void safeMutexLock(pthread_mutex_t &mutex)
{
    int ret;
    ret = pthread_mutex_lock(&mutex);
    failureCheckerPrinter(MUTEX_LOCK, ret);
}
/*
 * Safely unlock a mutex - check return value and operate accordingly using the
 * method failureCheckerPrinter.
 */

void safeMutexUnLock(pthread_mutex_t &mutex)
{
    int ret;
    ret = pthread_mutex_unlock(&mutex);
    failureCheckerPrinter(MUTEX_UNLOCK, ret);
}
/*
 * Safely destroy a mutex.
 */
void safeMutexDestroy(pthread_mutex_t &mutex)
{
    int ret;
    ret = pthread_mutex_destroy(&mutex);

    failureCheckerPrinter(MUTEX_DESTROY, ret);
}
/*
 * Safely signal some thread that is blocked - check return value and operate
 * accordingly using method failureCheckerPrinter.
 */
void safeCondDestroy(pthread_cond_t &cond)
{
    int ret;
    ret = pthread_cond_destroy(&cond);

    failureCheckerPrinter(COND_DESTROY, ret);
}
/*
 * Safely signal some thread that is blocked - check return value and operate
 * accordingly using method failureCheckerPrinter.
 */
void safeCondVarSignal(pthread_cond_t &cond)
{
    int ret;
    ret = pthread_cond_signal(&cond);
    failureCheckerPrinter(COND_SIGNAL, ret);
}
/*
 * Safely put a thread to sleep until other thread will wake him up using a
 * signal (if not, the thread will be woken up automatically after 0.01
 * seconds) - check return value and operate accordingly using the method
 * failureCheckerPrinter.
 */
void safeCondTimedWait()
{
    int ret;

    struct timespec timeToWait;
    ret = clock_gettime(CLOCK_REALTIME, &timeToWait);
    failureCheckerPrinter(CLOCK, ret);

    timeToWait.tv_sec = (timeToWait.tv_sec*CONT_WAIT_TO_SEC + \
                         CONT_WAIT_TIME)/CONT_WAIT_TO_SEC;

    ret = pthread_cond_timedwait(&shufflerCV, &shufflerMutex, &timeToWait);

    if(!(ret == SUCCESS || ret == ETIMEDOUT)) // ETIMEDOUT- times up
    {
        failureCheckerPrinter(COND_T_WAIT, FAILURE);
    }

}

//----------------------------------MAP---------------------------------------//

/**
 * For a less busy code, implemented a sub routine that calculates
 */
int lowerBoundIndexCalculator(int upperBoundIndex)
{
    if(upperBoundIndex < CHUNK_OF_TASKS)
    {
        return 0;
    }
    return (upperBoundIndex - CHUNK_OF_TASKS) + 1;
}

/*
 * The Map function is applied in parallel to every <k1,v1> pair in the input
 * dataset This produces a list of pairs of <key2,value2> for each call via
 * emit2.
 */
void* execMap(void* /*args*/)
{
    // start map tasks
    int upperBoundIndex, lowerBoundIndex, readFromIndex, readToIndex;

    // All map workers will wait until the main thread will finish creating map.
    safeMutexLock(threadContainerMutex);
    safeMutexUnLock(threadContainerMutex);

    while (true) {
        safeMutexLock(mapIndexMutex);

        // update the thread's available index and the global one
        upperBoundIndex = itemsToMapVectorIndex;
        itemsToMapVectorIndex -= CHUNK_OF_TASKS;

        safeMutexUnLock(mapIndexMutex);

        // check if the task vector is empty
        if (upperBoundIndex + CHUNK_OF_TASKS > 0 && upperBoundIndex >= 0)
        {
            lowerBoundIndex = lowerBoundIndexCalculator(upperBoundIndex);

            // This will be the first index after adding pairs of <k2,v2>
            readFromIndex = (int)mapThreadContainerMap[pthread_self()].size();

            // Run map on the available indices.
            for (int i = upperBoundIndex; i >= lowerBoundIndex; --i)
            {
                functions->Map(itemsToMapVector[i].first, \
                               itemsToMapVector[i].second);
            }

            // Finished inserting pairs of <k2,v2> via emit2.
            readToIndex = (int)mapThreadContainerMap[pthread_self()].size() - \
                          LAST_INDEX;

            // Add task while locking shared memory
            safeMutexLock(shufflerMutex);
            shufflerTasks.push_back(std::make_pair(pthread_self(), \
                                    std::make_pair(readFromIndex, \
                                    readToIndex)));
            // Its time to notify the shuffler that a task's waiting.
            safeCondVarSignal(shufflerCV);
            safeMutexUnLock(shufflerMutex);
        }
        else
        {
            // Count Working thread that finished their assignment.
            safeMutexLock(mapThreadsFinishedMutex);
            mapThreadsFinished++;
            safeMutexUnLock(mapThreadsFinishedMutex);

            timeStamp(MAPPER, TERMINATE);
            pthread_exit(nullptr);
        }
    }
}

/*
 * This function is called by the Map function (implemented by the user) in
 * order to add a new pair of <k2,v2> to the framework's internal data
 * structures. (Corresponding <k2,v2> container to the calling thread.
 */
void Emit2 (k2Base* a, v2Base* b)
{
    safeMutexLock(mapThreadMutexMap[pthread_self()]);
    mapThreadContainerMap[pthread_self()].push_back(std::make_pair(a, b));
    safeMutexUnLock(mapThreadMutexMap[pthread_self()]);
}

//----------------------------------Shuffler----------------------------------//

/*
 * Condition variables are prone to "spurious wake-ups" - The code can
 * be unblocked. Therefore, Check with a loop if taskList is empty or done
 * mapping <key1,value1>.
*/
void shufflerGoToSleep()
{
    safeMutexLock(shufflerMutex);
    safeCondTimedWait();
    safeMutexUnLock(shufflerMutex);
}

/**
 * The goal of the shuffle function is to merge all the pairs with the same
 * key (the final product is shuffledTasks).
 * The Shuffle converts a list of <k2, v2> to a list of <k2, list<v2>>
 * (using the properties of [] function), where each element in this list
 * has a unique key.
 */
void* execShuffle(void* /*args*/) {
    // Current <k2,v2> pair in thread's container.
    MAP_CONTAINER_ITEM currentPair;

    // Information in shuffler task pair.
    SHUFFLER_TASK currentTask;
    int readFromIndex, readToIndex;
    pthread_t currentThread;

    // If the shuffler's done with the mutual resource shufflerTasks - unlock.
    bool canUnlock = true;

    while (true)
    {
        safeMutexLock(shufflerMutex); // LOCK

        if (!shufflerTasks.empty())
        {
            currentTask = shufflerTasks.front();
            shufflerTasks.erase(shufflerTasks.begin());

            safeMutexUnLock(shufflerMutex); // UNLOCK

            canUnlock = false;

            // Obtain task information - thread's id and what indices to read.
            currentThread = currentTask.first;
            readFromIndex = currentTask.second.first;
            readToIndex = currentTask.second.second;

            safeMutexLock(mapThreadMutexMap[currentThread]);
            // Now going to handel treatedThread's container.
            for (int i = readFromIndex; i <= readToIndex; i++)
            {
                // Obtain a pair and delete it from thread's container.
                currentPair = (mapThreadContainerMap[currentThread])[i];

                // If a key already exists, the value is simply obtained.
                shuffledTasks[currentPair.first].push_back(currentPair.second);
            }
            safeMutexUnLock(mapThreadMutexMap[currentThread]);
        }
        // Shuffler's work's done if no tasks left and all mappers are done.
        if (shufflerTasks.empty() && (mapThreadsFinished == poolSize))
        {
            safeMutexUnLock(shufflerMutex); // UNLOCK
            shufflerMutex = PTHREAD_MUTEX_INITIALIZER;
            timeStamp(SHUFFLER, TERMINATE);
            pthread_exit(nullptr);
        }
        if(canUnlock)
        {
            safeMutexUnLock(shufflerMutex); // UNLOCK
        }

        shufflerGoToSleep();
        canUnlock = true;
    }
}

//----------------------------------Reduce------------------------------------//

/**
 * This is done exactly in the same manner as we executed the map function
 * efficiently using ExecMap. The only difference is that now the datat
 * structure we work with is a map (shuffledTasks) and not a list (
 */
void* execReduce(void* /*args*/)
{
    safeMutexLock(threadContainerMutex);
    safeMutexUnLock(threadContainerMutex);

    std::map<k2Base*, V2_LIST, customCmp>::iterator threadIterator;
    std::map<k2Base*, V2_LIST, customCmp>::iterator threadIteratorToStop;

    int howMuch;

    do {
        // Add offset so "--howMuch" will work as expected.
        howMuch = CHUNK_OF_TASKS + OFFSET;

        safeMutexLock(reduceIndexMutex);
        // update the thread's available index and the global one
        threadIterator = mainReduceIterator;

        // Its our responsibility to track whether iterator reached the end.
        while ((mainReduceIterator != shuffledTasks.end()) && --howMuch)
        {
            mainReduceIterator++;
        }
        threadIteratorToStop = mainReduceIterator;
        safeMutexUnLock(reduceIndexMutex);

        // As long as iterator haven't reached end, can be incremented.
        if ((threadIterator != shuffledTasks.end()))
        {
            while(threadIterator!=threadIteratorToStop)
            {
                functions->Reduce(threadIterator->first, \
                                  threadIterator->second);
                threadIterator++;
            }
        }
        else
        {
            timeStamp(REDUCER, TERMINATE);
            pthread_exit(nullptr);
        }
    }
    while (true);
}

/**
 * The Emit3 function is used by the Reduce function in order to add a pair of
 * <k3, v3> to the final output (reduceThreadContainerMap that later on will
 * be organized to the desired output. The rest of the details of Emit3 are
 * similar to the Emit2.
 */
void Emit3 (k3Base* a, v3Base* b)
{
    reduceThreadContainerMap[pthread_self()].push_back(std::make_pair(a,b));
}

//--------------------------------Gather--------------------------------------//

/**
 * A custom comparator that compares pair of <k3,v3> by the value of the key
 */
bool k3v3PairCompare(OUT_ITEM a, OUT_ITEM b)
{
    return *a.first < *b.first;
}

/**
 * Go over reduceThreadContainerMap and gather all pairs from <k3,v3> containers
 * that correspond to every reduce worker's id.
 */
void gatherAndSort()
{

    for(auto const &item : reduceThreadContainerMap)
    {
        for(auto const &k3v3Pair : item.second)
        {
            outputList.push_back(k3v3Pair);
        }
    }
    // Sort by keys, using custom comperator.
    outputList.sort(k3v3PairCompare);
}

//----------------------------------------------------------------------------//

/**
 * This function, according to threads type, creates the thread.
 * If it's mappers or reducers, the corresponding map will be initialized in
 * this step (Create  a corresponding container for every thread).
 */
void createWorkers(WorkerType type)
{
    int ret;

    if(type == SHUFFLER)
    {
        ret = pthread_create(&shuffler, nullptr, execShuffle, nullptr);
        failureCheckerPrinter(THREAD_CREATION, ret);

        timeStamp(SHUFFLER, CREATE);
        return;
    }

    // Implementing according to Nati's solution.
    safeMutexLock(threadContainerMutex);

    // Create mapper threads and their Container
    for (int i = 0; i < poolSize; ++i)
    {
        // Add new Thread
        if(type == REDUCER)
        {
            ret = pthread_create(&threads[i], nullptr, execReduce, nullptr);
            failureCheckerPrinter(THREAD_CREATION, ret);

            mapThreadContainerMap[threads[i]];
            mapThreadMutexMap[threads[i]] = PTHREAD_MUTEX_INITIALIZER;
        }
        else // MAPPER
        {
            ret = pthread_create(&threads[i], nullptr, execMap, nullptr);
            failureCheckerPrinter(THREAD_CREATION, ret);

            reduceThreadContainerMap[threads[i]];
        }

        timeStamp(type, CREATE);
    }

    safeMutexUnLock(threadContainerMutex);
}

/**
 * Every created thread should be "joined" - The main function will wait, before
 * continuing, until all joined threads will be terminated.
 */
void joinWorkers(WorkerType type)
{
    int ret;

    if(type == SHUFFLER)
    {
        ret = pthread_join(shuffler, nullptr);
        failureCheckerPrinter(THREAD_JOIN, ret);
    }
    else // MAPPER OR REDUCER
    {
        for (int j = 0; j < poolSize ; j++)
        {
            ret = pthread_join(threads[j], nullptr);
            failureCheckerPrinter(THREAD_JOIN, ret);
        }
    }
}

//----------------------------------------------------------------------------//

/**
 * Initialize all mutexes in the framework.
 */
void initializeMutexCondvar()
{
    mapThreadsFinishedMutex = PTHREAD_MUTEX_INITIALIZER;
    threadContainerMutex = PTHREAD_MUTEX_INITIALIZER;
    mapIndexMutex = PTHREAD_MUTEX_INITIALIZER;
    shufflerMutex = PTHREAD_MUTEX_INITIALIZER;
    reduceIndexMutex = PTHREAD_MUTEX_INITIALIZER;
    logMutex = PTHREAD_MUTEX_INITIALIZER;

    shufflerCV = PTHREAD_COND_INITIALIZER;
}

/**
 * Destroy all the mutexes in the framework at the end of the use.
 */
void destroyMutexCondVar()
{
    safeCondDestroy(shufflerCV);

    safeMutexDestroy(mapThreadsFinishedMutex);
    safeMutexDestroy(threadContainerMutex);
    safeMutexDestroy(mapIndexMutex);
    safeMutexDestroy(shufflerMutex);
    safeMutexDestroy(reduceIndexMutex);
    safeMutexDestroy(logMutex);

    for(auto elem: mapThreadMutexMap)
    {
        safeMutexDestroy(elem.second);
    }
}

/*
 * The framework will be executed multiple times. Therefore, all the global
 * variables and data structures should be re-initialized.
 */
void resetDataStructures(int multiThreadLevel)
{
    mapThreadsFinished = 0;
    mapThreadContainerMap.clear();
    itemsToMapVector.clear();
    shufflerTasks.clear();
    shuffledTasks.clear();
    reduceThreadContainerMap.clear();
    outputList.clear();

    initializeMutexCondvar();

    logFile.open(".MapReduceFramework.log", std::fstream::out |\
                                            std::fstream::in | \
                                            std::fstream::app);

    if(!logFile.is_open())
        failureCheckerPrinter(FILE_OPEN, FAILURE);

    logFile << "runMapReduceFramework started with " \
            << multiThreadLevel \
            << " threads\n";
}


OUT_ITEMS_LIST runMapReduceFramework(MapReduceBase& mapReduce, \
                                     IN_ITEMS_LIST& itemsList, \
                                     int multiThreadLevel)
{
    // Start measuring Map and shuffle
    auto start = std::chrono::high_resolution_clock::now();

    // Reset all data structures in case of a additional runs are made.
    resetDataStructures(multiThreadLevel);

    // Define poolSize (might be smaller than multiThreadLevel)
    poolSize = multiThreadLevel;

    // Convert the list to vector
    itemsToMapVector = { std::begin(itemsList), std::end(itemsList) };

    //printInput(itemsToMapVector);
    itemsToMapVectorIndex = (int)itemsToMapVector.size() - LAST_INDEX;

    threads = new (std::nothrow) pthread_t [poolSize];
    if (threads == nullptr)
    {
        failureCheckerPrinter(MEMORY_ALOC, FAILURE);
    }

    functions = &mapReduce;

    // Run Mappers and Shuffler
    createWorkers(MAPPER);
    createWorkers(SHUFFLER);

    joinWorkers(MAPPER);
    joinWorkers(SHUFFLER);

    // Finish measuring Map and shuffle
    auto finish = std::chrono::high_resolution_clock::now();

    auto MapShuffleTime = std::chrono::duration_cast \
               <std::chrono::nanoseconds>(finish-start).count();

    // Start measuring Reduce
    start = std::chrono::high_resolution_clock::now();
    // After shuffler's work is done. Initialize an iterator on his output.
    mainReduceIterator = shuffledTasks.begin();

    // Run Reduce
    createWorkers(REDUCER);
    joinWorkers(REDUCER);

    gatherAndSort();

    delete[] threads;
    destroyMutexCondVar();

    // finish measuring Reduce
    finish = std::chrono::high_resolution_clock::now();

    // Write final information to the log file.
    logFile << "Map and Shuffle took " << std::chrono::duration_cast \
               <std::chrono::nanoseconds>(finish-start).count() << " ns\n";
    logFile << "Reduce took " << MapShuffleTime << " ns\n";
    logFile << "runMapReduceFramework finished\n";

    logFile.close();

    // Return the output
    return outputList;
}