#include <stdio.h>
#include <stdlib.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <string.h>

#define SERVER_PORT 8080
#define MAX_TOPIC_LEN 512
#define STX '\x02'
#define TOPIC_SEPARATOR '/'
#define WILD_CARD "#"

void print_usage(char *argv[]) {
    printf("Usage: '%s broker topic%csubtopic'\n\n"
           "Wildcards ('%s') are supported for topics and subtopics.\n"
           "Giving only a topic (e.g. '%s example.com example_topic' is equal to subscribing to 'example_topic%c#'\n",
           argv[0], TOPIC_SEPARATOR, WILD_CARD, argv[0], TOPIC_SEPARATOR);
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
void validate_args(int argc, char *argv[], char **hostname, char **topic, char **subtopic) {
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

    *hostname = argv[1];

    if (strcmp(argv[2], "") == 0) {
        fprintf(stderr,
                "Topic can't be empty.\n");
        exit(EXIT_FAILURE);
    }

    *topic = argv[2];
    if (!(*subtopic = spilt_at(*topic, TOPIC_SEPARATOR))) {
        *subtopic = "#";
    }

    int t;
    if ((t = strcmp(*topic, "") == 0) || strcmp(*subtopic, "") == 0) {
        fprintf(stderr,
                "%s can't be empty.\n", t ? "Topic" : "Subtopic");
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char *argv[]) {
    char buf[MAX_TOPIC_LEN * 2 + 1];
    char *hostname;
    char *topic, *subtopic, *msg;
    struct sockaddr_in *server_addr;
    int srv_fd, errcode;
    uint addr_length;
    ssize_t nbytes;

    validate_args(argc, argv, &hostname, &topic, &subtopic);

    server_addr = resolve_hostname(hostname);
    server_addr->sin_port = htons(SERVER_PORT);
    addr_length = sizeof(*server_addr);

    srv_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (srv_fd < 0) {
        perror("Error creating socket");
        return EXIT_FAILURE;
    }

    errcode = connect(srv_fd, (const struct sockaddr *) server_addr, addr_length);
    if (errcode < 0) {
        perror("Error connecting to server");
        return EXIT_FAILURE;
    }

    sprintf(buf, "s%s%c%s", topic, TOPIC_SEPARATOR, subtopic);
    buf[strlen(buf) < MAX_TOPIC_LEN * 2 + 1 ? strlen(buf) : MAX_TOPIC_LEN * 2] = '\0';

    send(srv_fd, buf, strlen(buf), 0);

    while (1) {
        nbytes = recv(srv_fd, buf, sizeof(buf), 0);
        buf[nbytes] = '\0';

        topic = buf;
        msg = spilt_at(buf, STX);

        printf("[%s] %s\n", topic, msg);
    }

    return EXIT_SUCCESS;
}
