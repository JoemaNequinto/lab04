/* 
    Author: Joe Ma Aubrey A. Nequinto
            2014-71937
            CMSC 180
    Program: Distributing Parts of a Matrix over Sockets
    Compile: gcc -pthread -o lab04 lab04.c
    Run Master: ./lab04 <N> <P> 0
    Run Slave: ./lab04 <N> <P> 1

*/
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#define CORE_AFFINED 0

typedef struct Parcels {
    int sockfd;
    // submatrix
    int row;
    int column;
} Parcel;

int connectToSlave(int* sockfd, struct sockaddr_in* server, char* ipAddress, int ports, int i);
Parcel* newParcel(int sockfd, int row, int column);
void* sendMatrix(void* p);

int main(int argc, char* argv[]) {

    /* Master Variables : S == 0 */
    int** matrix = NULL;
    char **ipAddress = NULL; 
    int *ports = NULL;
    
    int *sockfd = NULL;
    struct sockaddr_in *server = NULL;
    
    int i, j, counter, N, P, S, T;

    int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
    pthread_attr_t *attr = NULL;
    cpu_set_t *cpus = NULL;
    pthread_t *ps = NULL;

    Parcel *p = NULL;
    int row = 0, column = 0;

    /* Slave Variables : S == 1 */
    int socket_desc, client_sock, c, read_size;
    struct sockaddr_in slave, master;

    /* Both */
    FILE *fp = NULL;
    char buff[255];
    struct timeval start, end;

    if (argc != 4) {
        printf("Usage: ./lab04 <N> <P> <S>\n");
        printf("N - size of square matrix\nP - port number\nS - status of the instance\n");
        return 0;
    }

    N = atoi(argv[1]);      // size of square matrix
    P = atoi(argv[2]);      // port number
    S = atoi(argv[3]);      // status of the instance 0 = master, 1 = slave

    // Master
    if (S == 0) {
        /* Create a non-zero N * N square matrix M whose elements are assigned with random integer */
        matrix = (int**)malloc(sizeof(int*) * N);
        for (i = 0; i < N; i++) {
            matrix[i] = (int*)malloc(sizeof(int) * N);
        }
        for (i = 0; i < N; i++) {
            for (j = 0; j < N; j++) {
                matrix[i][j] = rand() % 100;
            }
        }
        /* Read the configuration file to determine the IP addresses and ports of the slaves and the number of slaves t */
        fp = fopen("config.in", "r");
        fscanf(fp, "%s", buff);     // master ip address
        fscanf(fp, "%s", buff);     // number of slaves - string
        T = atoi(buff);     // number of slaves - integer
        
        ipAddress = (char**)malloc(sizeof(char*) * T);
        for (i = 0; i < T; i++) {
            ipAddress[i] = (char*)malloc(10 * sizeof(char));
        }
        ports = (int*)malloc(sizeof(int) * T);
        
        counter = 0;
        while(counter < T) {
            fscanf(fp, "%s", ipAddress[counter]);
            fscanf(fp, "%s", buff);
            ports[counter] = atoi(buff);
            counter++;
        }
        fclose(fp);

        /* Divide your M into t submatrices of size n * n/t each, m1, m2, ..., mt */


        /* Take note of the system time time_before */
        gettimeofday(&start, NULL);

        attr = (pthread_attr_t*)malloc(sizeof(pthread_attr_t) * T);
        cpus = (cpu_set_t*)malloc(sizeof(cpu_set_t) * T);
        ps = (pthread_t*)malloc(sizeof(pthread_t) * T);
        
        /* Distribute the t submatrices to the corresponding t slaves by opening the port p 
         and initiating communication with the IP and port of each slave */
        sockfd = (int*)malloc(sizeof(int) * T);
        server = (struct sockaddr_in*)malloc(sizeof(struct sockaddr_in) * T);
        // connect to all slaves
        for (i = 0; i < T; i++) {
            connectToSlave(&sockfd[i], &server[i], ipAddress[i], ports[i], i);
        }
        // for each slave
        for (i = 0; i < T; i++) {
            // generate a 'parcel' as params for the threaded function 'sendMatrix'
            // p = newParcel(sockfd[i], &(submatrix[i]), row, column);
            p = newParcel(sockfd[i], row, column);
            if (CORE_AFFINED) {
                // core-affined setting
                pthread_attr_init(&attr[i]);
                CPU_ZERO(&cpus[i]);
                CPU_SET(i%num_cores, &cpus[i]);
                pthread_attr_setaffinity_np(&attr[i], sizeof(cpu_set_t), &cpus[i]);
                pthread_create(&ps[i], &attr[i], sendMatrix, (void*)&p);
            } else {
                // non core-affined setting
                pthread_create(&ps[i], NULL, sendMatrix, (void*)&p);
            }
        }
        // wait for all threads
        for (i = 0; i < T; i++) {
            pthread_join(ps[i], NULL);
        }

        /* Receive the acknowledgement "ack" from each slave, for all slaves t */

        /* Wait when all t slaves have sent their respective acknowledgements */

        /* Take note of the system time time_after */
        gettimeofday(&end, NULL);

        printf("start: %ld secs, %ld usecs\n", start.tv_sec, start.tv_usec);
        printf("end: %ld secs, %ld usecs\n", end.tv_sec, end.tv_usec);
        
        /* Obtain the elapsed time: time_after - time_before
           Output the time_elapsed at each instance's terminal */

        double elapsed = (end.tv_sec - start.tv_sec) + ((end.tv_usec - start.tv_usec)/1000000.0);
        printf("elapsed: %lf secs\n", elapsed);

    }
    // Slaves
    else if (S == 1) {
        /* Read from the configuration file what is the IP address of the master */
        fp = fopen("config.in", "r");
        fscanf(fp, "%s", buff);     // master ip address
        master.sin_addr.s_addr = inet_addr(buff);
        fclose(fp);

        /* Wait for the master to initiate an open port communication with it by listening to the 
           port assigned by the configuration file */
        // Create socket
        socket_desc = socket(AF_INET, SOCK_STREAM, 0);
        if (socket_desc == -1) {
            printf("Could not create socket");
        }
        puts("Socket created");

        // Prepare the sockaddr_in structure
        slave.sin_family = AF_INET;
        slave.sin_addr.s_addr = INADDR_ANY;
        slave.sin_port = htons(P);

        // Bind the socket
        if (bind(socket_desc, (struct sockaddr*)&slave, sizeof(slave)) < 0) {
            // print the error message
            perror("bind failed. Error");
            return 1;
        }
        puts("bind done");
     
        // lsiten to the socket
        listen(socket_desc, 3);
     
        puts("Waiting for incoming connections...");
        c = sizeof(struct sockaddr_in);
     
        /* When the master has initiated, take note of time_before */
        // accept connection from an incoming client
        client_sock = accept(socket_desc, (struct sockaddr*)&master, (socklen_t*)&c);
     
        if (client_sock < 0) {
            perror("accept failed");
            return 1;
        }
     
        puts("Connection accepted");
        gettimeofday(&start, NULL);

        /* Receive from the master the submatrix mi assigned to it */


        /* Send an acknowledgement "ack" to the master once the submatrix have been received fully */

        /* Take note of time_after */
        gettimeofday(&end, NULL);

        printf("start: %ld secs, %ld usecs\n", start.tv_sec, start.tv_usec);
        printf("end: %ld secs, %ld usecs\n", end.tv_sec, end.tv_usec);

        /* Obtain the elapsed time: time_after - time_before
           Output the time_elapsed at each instance's terminal */
        double elapsed = (end.tv_sec - start.tv_sec) + ((end.tv_usec - start.tv_usec)/1000000.0);
        printf("elapsed: %lf secs\n", elapsed);
    }

    return 0;
}

int connectToSlave(int* sockfd, struct sockaddr_in* server, char* ipAddress, int ports, int i) {
    *sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (*sockfd == -1) {
        printf("slave %d: could not create socket\n", i);
    }
    printf("slave %d: socket created\n", i);

    server->sin_addr.s_addr = inet_addr(ipAddress);
    server->sin_family = AF_INET;
    server->sin_port = htons(ports);
    // Connect to remote server
    if (connect(*sockfd, (struct sockaddr*)server, sizeof(*server)) < 0) {
        perror("connect failed. Error");
        return 1;
    }
 
    puts("Connected\n");
}

Parcel* newParcel(int sockfd, int row, int column) {
    Parcel *temp;

    temp = (Parcel*)malloc(sizeof(Parcel));
    temp->sockfd = sockfd;
    temp->row = row;
    temp->column = column;

    return temp;
}

void* sendMatrix(void* p) {

    Parcel *parcel = (Parcel *)p;
    int sockfd = parcel->sockfd;
    int *submatrix = NULL;
    int row = parcel->row;
    int column = parcel->column;

    if (send(parcel->sockfd, &submatrix, (row*column) * sizeof(int), 0) < 0) {
        puts("Send failed");
    }
}