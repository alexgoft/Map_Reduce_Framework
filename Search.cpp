#include <iostream>
#include <dirent.h>
#include <vector>

#include "MapReduceClient.h"
#include "MapReduceFramework.h"

using namespace std;

#define U_ERROR "Usage: <substring to search> <folders, separated by space>\"."
#define QUERY_ARGUMENT 1
#define NUM_OF_THREADS 20

// This are vectors that hold all of the pointers to object in the 2nd level.
vector<k2Base*> vecKey2;
vector<v2Base*> vecValue2;

IN_ITEMS_LIST inList;
OUT_ITEMS_LIST outList;

//-------------------------------------1--------------------------------------//

/**
 * This class inherits from k1Base. It represents the word given as argument.
 * that will be sent to map().
 */
class WordKey1 : public k1Base
{
public:
    /**
     * Construct this object.
     */
    WordKey1(string searchMe)
            :theWord(searchMe)
    { }
    /**
     * Implement '<'. Strings will be compared lexicographically.
     */
    bool operator<(const k1Base &other) const
    {
        WordKey1 word = (WordKey1&) other;
        return (this->theWord < word.theWord);
    }

    //-------DATA MEMBERS-------//

    /** The path to the Directory. **/
    string theWord;
};

/**
 * This class inherits from k1Base. It represents the string given as argument.
 * that will be sent to map().
 */
class PathValue1 : public v1Base
{
public:
    /**
     * Construct this object.
     */
    PathValue1(string searchHere)
            :thePath(searchHere)
    { }

    /** The path to the Directory. **/
    string thePath;
};

//-------------------------------------2--------------------------------------//

/**
 * This class inherits from k2Base. It represents the word given as argument.
 * that will be sent to map().
 */
class WordKey2 : public k2Base
{
public:
    /**
     * Construct this object.
     */
    WordKey2(string searchMe)
            :theWord(searchMe)
    { }
    /**
     * Implement '<'. Strings will be compared lexicographically.
     */
    bool operator<(const k2Base &other) const
    {
        WordKey2 word = (WordKey2&) other;
        return (this->theWord < word.theWord);
    }

    //-------DATA MEMBERS-------//

    /** The path to the Directory. **/
    string theWord;
};

/**
 * This class inherits from k1Base. It represents the string given as argument.
 * that will be sent to map().
 */
class FileValue2 : public v2Base
{
public:
    /**
     * Construct this object.
     */
    FileValue2(string searchHere)
            :theFile(searchHere)
    { }

    /** The path to the Directory. **/
    string theFile;
};

//-------------------------------------2--------------------------------------//

/**
 * This class inherits from k2Base. It represents the word given as argument.
 * that will be sent to map().
 */
class WordKey3 : public k3Base
{
public:
    /**
     * Construct this object.
     */
    WordKey3(string searchMe)
            :theWord(searchMe)
    { }
    /**
     * Implement '<'. Strings will be compared lexicographically.
     */
    bool operator<(const k3Base &other) const
    {
        WordKey3 word = (WordKey3&) other;
        return (this->theWord < word.theWord);
    }

    //-------DATA MEMBERS-------//

    /** The path to the Directory. **/
    string theWord;
};

/**
 * This class inherits from k3Base. It represents the file in which a word exists.
 * that will be sent to map().
 */
class FileValue3 : public v3Base
{
public:
    /**
     * Construct this object.
     */
    FileValue3(string searchHere)
            :theFile(searchHere)
    { }

    /** The path to the Directory. **/
    string theFile;
};

//----------------------------------------------------------------------------//

class SearchMapReduce : public MapReduceBase {
public:
    /**
     * Here we shall create list of <word,file of a the path given in v1Base>
     */
    virtual void Map(const k1Base *const key, const v1Base *const val) const
    {

        string word = ((WordKey1*) key)->theWord;
        string path = ((PathValue1*) val)->thePath;

        // Clean ups on the go - key1 and val1;
        //delete key;
        //delete val;

        DIR *dp;
        struct dirent *dirp;

        // ignore invalid paths
        if((dp  = opendir((path).c_str())) == NULL) {
            closedir(dp);
            return;
        }
        // Just pair word with file path.
        while ((dirp = readdir(dp)) != NULL) {
            WordKey2* key2 = new WordKey2(word);
            vecKey2.push_back(key2);

            FileValue2* value2 = new FileValue2(dirp->d_name);
            vecValue2.push_back(value2);

            Emit2(key2, value2);
        }
        delete dirp;
        closedir(dp);
    }

    /**
     * Shuffle outputs a list of <word, LIST of files in a given path>.
     */
    virtual void Reduce(const k2Base *const key, const V2_LIST &vals) const
    {

        string wordInPath;

        string givenWord = ((WordKey2*) key)->theWord;
        // Cleanups
        // delete key;

        //If a word in a given file, pair it with the word and Emit3 it.
        for (auto ci = vals.begin(); ci != vals.end(); ++ci)
        {

            wordInPath = ((FileValue2*) *ci)->theFile;

            // Search word in file's name.
            if ((wordInPath.find(givenWord) != string::npos))
            {
                Emit3(new WordKey3(givenWord), new FileValue3(wordInPath));
            }
        }
    }
};

//----------------------------------------------------------------------------//

void freeMemory(){
    // free its allocated memory
    for (auto ci : outList) {
        if (ci.first) {
            delete ci.first;
            ci.first = nullptr;
        }
        if (ci.second) {
            delete ci.second;
            ci.second = nullptr;
        }
    }

    // free input allocated memory
    for (auto item : inList) {

        if (item.first) {
            delete item.first;
            item.first = nullptr;
        }
        if (item.second) {
            delete item.second;
            item.second = nullptr;
        }
    }

    // free allocated memory
    for (auto item : vecValue2) {

        if (item) {
            delete item;
            item = nullptr;
        }
    }

    // free allocated memory
    for (auto item : vecKey2) {

        if (item) {
            delete item;
            item = nullptr;
        }
    }
}
int main(int argc, char* argv[]) {

    // check args
    if (argc <= QUERY_ARGUMENT) {
        cerr << U_ERROR;
        exit(EXIT_FAILURE);
    }

    string query = argv[QUERY_ARGUMENT];

    // Make list of pair. This will map()'s input
    for (int i = 2; i < argc; ++i) {
        inList.push_front(make_pair(new WordKey1(query), \
                                    new PathValue1(argv[i])));
    }

    // set map and reduce functions
    SearchMapReduce mapReduce;

    // run framework
    outList = runMapReduceFramework(mapReduce, \
                                                   inList, NUM_OF_THREADS);

    // print output
    for (auto ci : outList) {
        cout << ((FileValue3 *) (ci.second))->theFile << " ";
    }


    // free all allocated memory
    freeMemory();
    return 0;
}