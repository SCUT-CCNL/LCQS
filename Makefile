CXX=g++
CPPFLAGS+=-Dunix
CXXFLAGS+=-O3
CXXFLAGS+=-std=c++11

all: lcqs

libzpaq.o: libzpaq.cpp libzpaq.h
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ -c libzpaq.cpp

lcqs.o: lcqs.cpp lcqs.h libzpaq.h
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -w -o $@ -c lcqs.cpp -pthread

main.o: main.cpp lcqs.h
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ -c main.cpp -pthread

lcqs: main.o lcqs.o libzpaq.o
	$(CXX) $(LDFLAGS) -o $@ main.o lcqs.o libzpaq.o -pthread

clean:
	rm -f main.o lcqs.o libzpaq.o lcqs

