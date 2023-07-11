#include <stdio.h>
#include <stdlib.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <string.h>
#include <errno.h>

#define SERVER_PORT 8080
#define MSG_BUF_SIZE 4096
#define MAX_TOPIC_LEN 512
#define STX '\x02'
#define TOPIC_SEPARATOR '/'
#define WILD_CARD "#"
#define TIMEOUT_SECS 15

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
    if ((t = strlen(*topic) > MAX_TOPIC_LEN) || strlen(*subtopic) > MAX_TOPIC_LEN) {
        fprintf(stderr, "%s to long! Max length is %d.", t ? "Topic" : "Subtopic", MAX_TOPIC_LEN);
        exit(EXIT_FAILURE);
    }

    if ((t = strcmp(*topic, "") == 0) || strcmp(*subtopic, "") == 0) {
        fprintf(stderr,
                "%s can't be empty.\n", t ? "Topic" : "Subtopic");
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char *argv[]) {
    char buf[MSG_BUF_SIZE];
    char *hostname;
    char *topic, *subtopic, *msg;
    struct sockaddr_in *server_addr;
    struct timeval tv;
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

    tv.tv_sec = TIMEOUT_SECS;
    tv.tv_usec = 0;
    setsockopt(srv_fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));

    snprintf(buf, sizeof(buf), "s%s%c%s", topic, TOPIC_SEPARATOR, subtopic);

    puts("Sending subscription request to broker...");
    do {
        nbytes = send(srv_fd, buf, strlen(buf), 0);
        if (nbytes == -1) {
            perror("send sub request");
            return EXIT_FAILURE;
        }
        nbytes = recv(srv_fd, buf, sizeof(buf), 0);
        if (nbytes == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            puts("Didn't receive an acknowledge from the broker. Sending request again...");
        } else if (nbytes == -1) {
            perror("recv ack");
            return EXIT_FAILURE;
        }
    } while (nbytes == -1 && (errno == EAGAIN || errno == EWOULDBLOCK));

    if (nbytes >= 0) {
        buf[nbytes] = '\0';
        char cmd = buf[0];
        char *t = &buf[1];
        if (cmd == 'A') {
            char *st = spilt_at(t, TOPIC_SEPARATOR);
            if (strcmp(t, topic) == 0 && strcmp(st, subtopic) == 0) {
                puts("Request was acknowledged by the broker!");
            } else {
                puts("Couldn't confirm a successful request! Exiting...");
                return EXIT_FAILURE;
            }
        }
    } else {
        perror("recv ack");
        return EXIT_FAILURE;
    }

    tv.tv_sec = 0;
    tv.tv_usec = 0;
    setsockopt(srv_fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));

    while (1) {
        nbytes = recv(srv_fd, buf, sizeof(buf), 0);
        if (nbytes == -1) {
            perror("recv msg");
        } else {
            buf[nbytes] = '\0';

            topic = buf;
            msg = spilt_at(buf, STX);

            printf("[%s] %s\n", topic, msg);
        }
    }
}
