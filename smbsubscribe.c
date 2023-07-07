#include <stdio.h>
#include <stdlib.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <string.h>

#define SERVER_PORT 8080
#define CLIENT_PORT 8081
#define MAX_TOPIC_LEN 512
#define MAX_ARGS 2 // Maximal number of arguments that are needed for any given command and thus send to the server.

void print_usage(char *argv[]) {
    printf("Usage: '%s server command [argument1 [argument2]]'\n", argv[0]);
}

// Resolves a hostname string to the corresponding IP address.
struct sockaddr_in* resolve_hostname(char *hostname) {
    struct addrinfo hints;
    struct addrinfo *res;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;

    int errcode = getaddrinfo(hostname, NULL, &hints, &res);
    if (errcode != 0) {
        fprintf(stderr, "getaddrinfo: %s", gai_strerror(errcode));
        exit(EXIT_FAILURE);
    }

    return (struct sockaddr_in *) res->ai_addr;
}

// Very liberally check arguments for validity to stop the client from sending commands that can't possibly be
// fulfilled to the server. Only checks the number of provided arguments. If not enough arguments for the given command
// are provided, exit with failure. Further error handling and reporting is left to the server.
void validate_args(int argc, char *argv[]) {
    if (argc == 1) {
        print_usage(argv);
        exit(EXIT_SUCCESS);
    }

    if (argc < 3) {
        fprintf(stderr,
                "You need to supply at least 2 arguments but you provided %d.\n", argc-1);
        print_usage(argv);
        exit(EXIT_FAILURE);
    }
    else if (strcmp(argv[2], "pwd") == 0 || strcmp(argv[2], "dir") == 0) { return; }
    else if (strcmp(argv[2], "cd") == 0 && argc < 4) {
        fprintf(stderr,
                "Missing directory argument for cd command.\n");
        print_usage(argv);
        exit(EXIT_FAILURE);
    } else if ((strcmp(argv[2], "get") == 0 || strcmp(argv[2], "put") == 0) && argc < 4) {
        fprintf(stderr,
                "Missing file argument for %s command.\n", argv[2]);
        print_usage(argv);
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char *argv[]) {
    char buf[MAX_TOPIC_LEN + 1];
    char *hostname;
    char *topic;
    struct sockaddr_in *server_addr, rcv_addr;
    int send_fd, rcv_fd, errcode;
    uint addr_length;
    ssize_t nbytes;

    // validate_args(argc, argv);

    // to_send = argc - 2;
    // if (to_send > MAX_ARGS + 1) { to_send = MAX_ARGS + 1; }

    hostname = argv[1];
    topic = argv[2];

    server_addr = resolve_hostname(hostname);
    server_addr->sin_port = htons(SERVER_PORT);
    addr_length = sizeof(*server_addr);

    send_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (send_fd < 0) {
        perror("Error creating socket");
        return EXIT_FAILURE;
    }

    errcode = connect(send_fd, (const struct sockaddr *) server_addr, addr_length);
    if (errcode < 0) {
        perror("Error connecting to server");
        return EXIT_FAILURE;
    }

    sprintf(buf, "s%s", topic);
    buf[strlen(buf) < MAX_TOPIC_LEN + 1 ? strlen(buf) : MAX_TOPIC_LEN] = '\0';

    sendto(send_fd, buf, strlen(buf), 0, (const struct sockaddr *) server_addr, addr_length);

    rcv_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (rcv_fd < 0) {
        perror("vlftpd: Error creating socket");
        return EXIT_FAILURE;
    }

    memset(&rcv_addr, 0, sizeof(rcv_addr));
    rcv_addr.sin_family = AF_INET;
    rcv_addr.sin_addr.s_addr = server_addr->sin_addr.s_addr;
    rcv_addr.sin_port = server_addr->sin_port = htons(CLIENT_PORT);

    errcode = bind(rcv_fd, (const struct sockaddr *) &rcv_addr, sizeof(rcv_addr));

    while (1) {
        nbytes = recvfrom(rcv_fd, buf, sizeof(buf), 0, NULL, NULL);
        buf[nbytes] = '\0';
        puts(buf);
    }

    return EXIT_SUCCESS;
}
