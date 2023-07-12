/**
 * smbpublish.c
 * Simple message broker publisher that publishes a message on a given topic based on program arguments.
 */

#include <stdio.h>
#include <stdlib.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <string.h>

#define SERVER_PORT 8080
#define MSG_BUF_SIZE 4096
#define MAX_TOPIC_LEN 512
#define SOH '\x01'              // Start of heading control char: Used to start a publish request message
#define STX '\x02'              // Start of text control char: Used to separate topic and message
#define TOPIC_SEPARATOR '/'     // Used to separate topic and subtopic
#define WILD_CARD "#"

void print_usage(char *argv[]) {
    printf("Usage: '%s broker topic/subtopic message'\n", argv[0]);
}

/**
 * Splits a string in two by replacing the first occurrence of sep with '\0'.
 *
 * @param str The string to split
 * @param sep The separator on which the split should occur
 * @return A pointer to the start of the second string or null if sep wasn't found
 */
char *spilt_at(char *str, char sep) {
    char *sep_ptr = strchr(str, sep);
    if (!sep_ptr) return NULL;
    sep_ptr[0] = 0;
    return sep_ptr + 1;
}

/**
 * Resolves a hostname or IP address string to the corresponding internet socket address.
 *
 * @param hostname The hostname or IP to resolve
 * @return The internet socket address struct
 */
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

/**
 * Checks the args for validity and saves them in the corresponding variables.
 */
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

    int t;
    if ((t = strlen(*topic) > MAX_TOPIC_LEN) || strlen(*subtopic) > MAX_TOPIC_LEN) {
        fprintf(stderr, "%s to long! Max length is %d.", t ? "Topic" : "Subtopic", MAX_TOPIC_LEN);
        exit(EXIT_FAILURE);
    }

    if (strcmp(*topic, WILD_CARD) == 0 || strcmp(*subtopic, WILD_CARD) == 0) {
        fprintf(stderr, "Usage of wildcard '%s' is not allowed!", WILD_CARD);
        exit(EXIT_FAILURE);
    }

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
    struct sockaddr_in *broker_addr;
    int broker_fd, errcode;
    uint addr_length;
    ssize_t nbytes;

    validate_args(argc, argv, &hostname, &topic, &subtopic, &msg);

    broker_addr = resolve_hostname(hostname);
    broker_addr->sin_port = htons(SERVER_PORT);
    addr_length = sizeof(*broker_addr);

    // Create UDP socket
    broker_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (broker_fd < 0) {
        perror("Error creating socket");
        return EXIT_FAILURE;
    }

    // Set the default address to send to and the only address to receive from
    errcode = connect(broker_fd, (const struct sockaddr *) broker_addr, addr_length);
    if (errcode < 0) {
        perror("Error connecting to server");
        return EXIT_FAILURE;
    }

    // Prepare publish message
    snprintf(buf, sizeof(buf), "%c%s%c%s%c%s", SOH, topic, TOPIC_SEPARATOR, subtopic, STX, msg);

    // Write message to broker
    nbytes = send(broker_fd, buf, strlen(buf), 0);
    if (nbytes == -1) {
        perror("send");
        return EXIT_FAILURE;
    } else if (nbytes != strlen(buf)) {
        fprintf(stderr, "Failed to send message '%s' on topic '%s%c%s' to %s:%d\n", msg, topic, TOPIC_SEPARATOR, subtopic, inet_ntoa(broker_addr->sin_addr),
               ntohs(broker_addr->sin_port));
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
