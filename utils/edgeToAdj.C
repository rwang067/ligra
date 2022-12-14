#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <cmath>
#include <sys/mman.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "parseCommandLine.h"

const ulong N = 1413511393L, M = 6636600779L;

void genAdjGraph(char* fname)
{
    char p[16], buff[20];
    ulong* offset = (ulong*)malloc(sizeof(ulong) * N);
    FILE* fp = fopen(fname, "r");
    char* suffix = "_adj"; // edges
    strcat(fname, suffix);
    int fd = open(fname, O_RDWR | O_CREAT, 0666);
    if (fd == -1) {
        perror("open");
        exit(-1);
    }
    ulong cur = 1, cnt, s, d;
    offset[0] = 0;
    // for(int i = 0; i < 7; i++)
    //     fscanf(fp, "%s", buff); // ignore comments in Friendster
    while(fscanf(fp, "%d%d", &s, &d) == 2)
    {
        while(s >= cur)
            offset[cur++] = cnt;
        cnt++;
        sprintf(buff, "%ld\n", d);
        write(fd, buff, strlen(buff));
    }
    fclose(fp);
    close(fd);
    printf("%ld\n", cnt);
    char* suffix1 = "_1"; //offsets
    strcat(fname, suffix1);
    fd = open(fname, O_RDWR | O_CREAT, 0666);
    if (fd == -1) {
        perror("open");
        exit(-1);
    }
    for(int i = 0; i < N; i++)
    {
        sprintf(buff, "%ld\n", offset[i]);
        write(fd, buff, strlen(buff));
    }
    close(fd);
    printf("Done\n");
}

int main(int argc, char* argv[]) {
  commandLine P(argc,argv,"<inFile>");
  char* iFile = P.getArgument(0);
  genAdjGraph(iFile);
  return 0;
}