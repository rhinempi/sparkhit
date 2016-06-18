CC=g++

#FLAGS= -ggdb -pg -O3 -static
FLAGS=-O3 -static -fopenmp

SOURCE = align refseq main param reads utilities 
OBJS= $(patsubst %,%.o,$(SOURCE))

all: sparkhit-frhit

%.o:%.cpp
	$(CC) $(FLAGS) -c $< -o $@
sparkhit-frhit: $(OBJS)
	$(CC) $(FLAGS) $^ -o $@ 

clean:
	rm -f *.o fr-hit

