#include <stdio.h>
#include <stdlib.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <string.h>

#define SERVER_PORT 8080
#define MSG_BUF_SIZE 2048
#define MAX_TOPIC_LEN 512
#define SOH '\x01'
#define STX '\x02'
#define TOPIC_SEPARATOR '/'
#define WILD_CARD "#"

void print_usage(char *argv[]) {
    printf("Usage: '%s broker topic/subtopic message'\n", argv[0]);
}

char *spilt_at(char *str, char sep) {
    char *sep_ptr = strchr(str, sep);
    if (!sep_ptr) return sep_ptr;
    sep_ptr[0] = 0;
    return sep_ptr + 1;
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
void validate_args(int argc, char *argv[], char **hostname, char **topic, char **subtopic, char** msg) {
    if (argc == 1) {
        print_usage(argv);
        exit(EXIT_SUCCESS);
    }

    if (argc < 4) {
        fprintf(stderr,
                "You need to supply at least 3 arguments but you provided %d.\n", argc-1);
        print_usage(argv);
        exit(EXIT_FAILURE);
    }

    *hostname = argv[1];

    if (strcmp(argv[2], "") == 0) {
        fprintf(stderr,
                "Topic can't be empty.\n");
        exit(EXIT_FAILURE);
    }

    *topic = argv[2];
    if (!(*subtopic = spilt_at(*topic, TOPIC_SEPARATOR))) {
        fprintf(stderr,"You need to provide a subtopic seperated with '%c'\n", TOPIC_SEPARATOR);
        exit(EXIT_FAILURE);
    }

    if (strcmp(*topic, WILD_CARD) == 0 || strcmp(*subtopic, WILD_CARD) == 0) {
        fprintf(stderr, "Usage of wildcard '%s' is not allowed!", WILD_CARD);
        exit(EXIT_FAILURE);
    }

    int t;
    if ((t = strcmp(*topic, "") == 0) || strcmp(*subtopic, "") == 0) {
        fprintf(stderr,
                "%s can't be empty.\n", t ? "Topic" : "Subtopic");
        exit(EXIT_FAILURE);
    }

    *msg = argv[3];
}

int main(int argc, char *argv[]) {
    char buf[MSG_BUF_SIZE];
    char *hostname;
    char *topic, *subtopic, *msg;
    struct sockaddr_in *server_addr;
    int sock_fd, errcode;
    uint addr_length;
    ssize_t nbytes;

    validate_args(argc, argv, &hostname, &topic, &subtopic, &msg);

    hostname = argv[1];
    topic = argv[2];
    msg = argv[3];

    server_addr = resolve_hostname(hostname);
    server_addr->sin_port = htons(SERVER_PORT);
    addr_length = sizeof(*server_addr);

    sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_fd < 0) {
        perror("Error creating socket");
        return EXIT_FAILURE;
    }

    errcode = connect(sock_fd, (const struct sockaddr *) server_addr, addr_length);
    if (errcode < 0) {
        perror("Error connecting to server");
        return EXIT_FAILURE;
    }

    snprintf(buf, sizeof(buf), "%c%s%c%s%c%s", SOH, topic, TOPIC_SEPARATOR, subtopic, STX, msg);

    nbytes = send(sock_fd, buf, strlen(buf), 0);
    if (nbytes == -1) {
        perror("smbpublish: send");
        return EXIT_FAILURE;
    } else if (nbytes != strlen(buf)) {
        fprintf(stderr, "smbpublish: Failed to send message '%s' on topic '%s%c%s' to %s:%d\n", msg, topic, TOPIC_SEPARATOR, subtopic, inet_ntoa(server_addr->sin_addr),
               ntohs(server_addr->sin_port));
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
