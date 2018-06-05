CC = g++
STD = -std=gnu++11

MAPREDUCE_CLIENT = MapReduceClient.h
MAPREDUCE_FRAMEWORK = MapReduceFramework.h MapReduceFramework.cpp
MAPREDUCE_LIBRARY = MapReduceFramework.a
SEARCH = Search.cpp
PICS = FirstComeFirstServeGanttChart.jpg PrioritySchedulingGanttChart.jpg\
    RoundRobinGanttChart.jpg ShortestRemainingTimeFirstGanttChart.jpg

TAROBJECTS = Search.cpp MapReduceFramework.cpp Makefile README $(PICS)

CFLAGS = -Wextra -Wvla -Wall -pthread

all: MapReduceFramework Search

MapReduceFramework: $(MAPREDUCE_CLIENT) $(MAPREDUCE_FRAMEWORK)
	${CC} $(STD) ${CFLAGS} -c MapReduceFramework.cpp -o MapReduceFramework.o
	ar rcs MapReduceFramework.a MapReduceFramework.o

Search: $(SEARCH) $(MAPREDUCE_LIBRARY)
	${CC} $(STD) ${CFLAGS} -c Search.cpp -o Search.o
	${CC} $(STD) ${CFLAGS} Search.o -L. MapReduceFramework.a -o Search

tar:
	tar cvf ex3.tar ${TAROBJECTS}

clean:
	rm -f ex3.tar MapReduceFramework.o Search.o MapReduceFramework.a \
	.MapReduceFramework.log Search

.PHONY: all MapReduceFramework tar clean
