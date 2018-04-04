/* 
    Author: Joe Ma Aubrey A. Nequinto
            2014-71937
            CMSC 180
    Program: Distributing Parts of a Matrix over Sockets
    Compile: gcc -pthread lab04.c -o lab04
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

#define CORE_AFFINED 1

typedef struct Parcels {
    int sockfd;
    int *submatrix;
    int number;
} Parcel;

int connectToSlave(int* sockfd, struct sockaddr_in* server, char* ipAddress, int ports, int i);
Parcel * newParcel(int sockfd, int* submatrix, int number);
void * sendMatrix(void* p);
void deleteMatrix(int *matrix);

int main(int argc, char* argv[]) {

    int i, j, N, P, S, T;

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
        /* Master Variables : S == 0 */
        int* matrix = NULL;
        char **ipAddress = NULL; 
        int *ports = NULL;
        
        int *sockfd = NULL;
        struct sockaddr_in *server = NULL;
        char server_reply[4];
        
        int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
        pthread_attr_t *attr = NULL;
        cpu_set_t *cpus = NULL;
        pthread_t *ps = NULL;

        Parcel *p = NULL;
        int count, remainder, prefixSum, temp;
        int **submatrix = NULL, *number = NULL;

        /* Create a non-zero N * N square matrix M whose elements are assigned with random integer */
        matrix = (int*)malloc(sizeof(int) * (N*N));
        printf("before\n");
        for (i = 0; i < N; i++) {
            for (j = 0; j < N; j++) {
                matrix[N * i + j] = rand() % 100;
                // printf("%d\t", matrix[N * i + j]);
            }
            // printf("\n");
        }
        printf("after\n");
        /* Read the configuration file to determine the IP addresses and ports of the slaves and the number of slaves t */
        fp = fopen("config.in", "r");
        fscanf(fp, "%s", buff);     // master ip address
        fscanf(fp, "%s", buff);     // number of slaves - string
        T = atoi(buff);     // number of slaves - integer
        
        ipAddress = (char**)malloc(sizeof(char*) * T);
        for (i = 0; i < T; i++) {
            ipAddress[i] = (char*)malloc(15 * sizeof(char));
        }
        ports = (int*)malloc(sizeof(int) * T);
        
        for (i = 0; i < T; i++) {
            fscanf(fp, "%s", ipAddress[i]);
            fscanf(fp, "%s", buff);
            ports[i] = atoi(buff);
        }
        fclose(fp);

        /* Divide your M into t submatrices of size n/t * n each, m1, m2, ..., mt */
        submatrix = (int **)malloc(sizeof(int*) * T);
        number = (int *)malloc(sizeof(int) * T);
        count = N / T;
        remainder = N - count * T;
        prefixSum = 0;
        for (i = 0; i < T; i++) {
            temp = (i < remainder) ? count + 1 : count;
            // submatrix[i] = matrix[prefixSum];
            int *k = malloc(sizeof(int));
            k = &matrix[prefixSum];
            submatrix[i] = k;
            number[i] = temp * N;
            prefixSum += number[i];
        }

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
            // send the number of integers in the matrix
            if (send(sockfd[i], &number[i], sizeof(number[i]), 0) < 0) {
                puts("Number Send failed");
            }
            // generate a 'parcel' as params for the threaded function 'sendMatrix'
            p = newParcel(sockfd[i], submatrix[i], number[i]);
            if (CORE_AFFINED) {
                // core-affined setting
                pthread_attr_init(&attr[i]);
                CPU_ZERO(&cpus[i]);
                CPU_SET(i%num_cores, &cpus[i]);
                pthread_attr_setaffinity_np(&attr[i], sizeof(cpu_set_t), &cpus[i]);
                pthread_create(&ps[i], &attr[i], sendMatrix, (void*)p);
            } else {
                // non core-affined setting
                pthread_create(&ps[i], NULL, sendMatrix, (void*)p);
            }
        }
        /* Receive the acknowledgement "ack" from each slave, for all slaves t */
        /* Wait when all t slaves have sent their respective acknowledgements */
        // wait for all threads
        for (i = 0; i < T; i++) {
            // Receive a reply from the server
            if (recv(sockfd[i], &server_reply, sizeof(server_reply), 0) < 0) {
                puts("recv failed");
                return 0;
            }
            printf("Server %d reply: %s\n", i, server_reply);
            pthread_join(ps[i], NULL);
            close(sockfd[i]);
        }

        /* Take note of the system time time_after */
        gettimeofday(&end, NULL);

        printf("start: %ld secs, %ld usecs\n", start.tv_sec, start.tv_usec);
        printf("end: %ld secs, %ld usecs\n", end.tv_sec, end.tv_usec);
        
        /* Obtain the elapsed time: time_after - time_before
           Output the time_elapsed at each instance's terminal */

        double elapsed = (end.tv_sec - start.tv_sec) + ((end.tv_usec - start.tv_usec)/1000000.0);
        printf("elapsed: %lf secs\n", elapsed);

        deleteMatrix(matrix);

    }
    // Slaves
    else if (S == 1) {
        /* Slave Variables : S == 1 */
        int socket_desc, client_sock, c, read_size;
        struct sockaddr_in slave, master;
        int *message = NULL;
        int number;
        char ack[4] = "ack";

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
        // Receive a message from client
        int rc;
        rc = recv(client_sock, &number, sizeof(number), 0);
        printf("number: %d\n", number);
        message = (int *)malloc(sizeof(int) * number);
        // read_size = recv(client_sock, message, number * sizeof(int), 0);
        // printf("read_size: %d\n", read_size);
        // Receive a message from client
        while ((read_size = recv(client_sock, message, number * sizeof(int), 0)) > 0) {
            printf("read_size: %d\n", read_size);
            write(client_sock, &ack, 4 * sizeof(char));
        }
        // for (i = 0; i < number; i++) {
        //     printf("%d ", message[i]);
        // }
        /* Send an acknowledgement "ack" to the master once the submatrix have been received fully */
        printf("Submatrix received. Acknowledgement \"ack\" sent.\n");
        // write(client_sock, &ack, 4 * sizeof(char));
     
        if (read_size == 0) {
            puts("Client disconnected");
        }
        else if (read_size == -1) {
            perror("recv failed");
        }

        /* Take note of time_after */
        gettimeofday(&end, NULL);

        printf("start: %ld secs, %ld usecs\n", start.tv_sec, start.tv_usec);
        printf("end: %ld secs, %ld usecs\n", end.tv_sec, end.tv_usec);

        /* Obtain the elapsed time: time_after - time_before
           Output the time_elapsed at each instance's terminal */
        double elapsed = (end.tv_sec - start.tv_sec) + ((end.tv_usec - start.tv_usec)/1000000.0);
        printf("elapsed: %lf secs\n", elapsed);

        deleteMatrix(message);
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

Parcel* newParcel(int sockfd, int* submatrix, int number) {
    Parcel *temp;

    temp = (Parcel*)malloc(sizeof(Parcel));
    temp->sockfd = sockfd;
    temp->submatrix = submatrix;
    temp->number = number;

    return temp;
}

void* sendMatrix(void* p) {

    Parcel *parcel = (Parcel *)p;
    int sockfd = parcel->sockfd;
    int *submatrix = parcel->submatrix;
    int number = parcel->number;

    if (send(sockfd, submatrix, number * sizeof(int), 0) < 0) {
        puts("Send failed");
    }
}

void deleteMatrix(int *matrix) {
    free(matrix);
}