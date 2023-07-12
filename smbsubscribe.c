/**
 * smbsubscribe.c
 * Simple message broker subscriber that subscribes to a topic and prints the received messages to the console.
 */

#include <stdio.h>
#include <stdlib.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <string.h>
#include <errno.h>

#define SERVER_PORT 8080
#define MSG_BUF_SIZE 4096
#define MAX_TOPIC_LEN 512
#define ACK 'A'                 // Used as the start of an ACKNOWLEDGE message
#define SUB 'S'                 // Used as the start of a SUBSCRIBE message
#define SOH '\x01'              // Start of heading control char: Used to start a PUBLISH request message
#define STX '\x02'              // Start of text control char: Used to separate topic and message
#define TOPIC_SEPARATOR '/'     // Used to separate topic and subtopic
#define WILD_CARD "#"
#define TIMEOUT_SECS 15         // Timeout for acknowledge response before new request is sent

/**
 * Prints usage information
 */
void print_usage(char *argv[]) {
    printf("Usage: '%s broker topic%csubtopic'\n\n"
           "Wildcards ('%s') are supported for topics and subtopics.\n"
           "Giving only a topic (e.g. '%s example.com example_topic' is equal to subscribing to 'example_topic%c#'\n",
           argv[0], TOPIC_SEPARATOR, WILD_CARD, argv[0], TOPIC_SEPARATOR);
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
        // If only the main topic was provided without a separator implicitly set subtopic to wildcard
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
    char cmd, *topic, *subtopic, *msg;
    struct sockaddr_in *broker_addr;
    struct timeval tv;
    int broker_fd, errcode;
    uint addr_length;
    ssize_t nbytes;

    validate_args(argc, argv, &hostname, &topic, &subtopic);

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

    // Set socket to timeout after TIMEOUT_SECS when not receiving an acknowledgment from the broker
    tv.tv_sec = TIMEOUT_SECS;
    tv.tv_usec = 0;
    setsockopt(broker_fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));

    // Build subscription message
    snprintf(buf, sizeof(buf), "%c%s%c%s", SUB, topic, TOPIC_SEPARATOR, subtopic);

    puts("Sending subscription request to broker...");

    // Send subscription message until broker acknowledges it to make sure the subscription was added to the broker
    do {
        nbytes = send(broker_fd, buf, strlen(buf), 0);
        if (nbytes == -1) {
            perror("send sub request");
            return EXIT_FAILURE;
        }
        nbytes = recv(broker_fd, buf, sizeof(buf), 0);
        if (nbytes == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) { // If recv timed out...
            puts("Didn't receive an acknowledge from the broker. Sending request again...");
        } else if (nbytes == -1) {
            perror("recv ack");
            return EXIT_FAILURE;
        }
    } while (nbytes == -1 && (errno == EAGAIN || errno == EWOULDBLOCK));

    // Check acknowledgement reply to see if subscription was added without error
    if (nbytes >= 0) {
        buf[nbytes] = '\0';
        cmd = buf[0];

        if (cmd == ACK || cmd == SOH) {
            char *t = &buf[1];
            char *st = spilt_at(t, TOPIC_SEPARATOR);

            if (strcmp(t, topic) == 0 && strcmp(st, subtopic) == 0) {
                if (cmd == ACK) {
                    puts("Request was acknowledged by the broker!\n");
                } else {
                    msg = spilt_at(st, STX);
                    puts("Request wasn't acknowledged by the broker but a message under the given topic and subtopic"
                         " was received. Request seems to have reached the server...\n");
                    printf("[%s] %s\n", topic, msg);
                }
            } else {
                puts("Couldn't confirm a successful request! Exiting...");
                return EXIT_FAILURE;
            }
        }
    } else {
        perror("recv ack");
        return EXIT_FAILURE;
    }

    // Set socket back to default no timeout behavior
    tv.tv_sec = 0;
    tv.tv_usec = 0;
    setsockopt(broker_fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));

    while (1) { // Listen for messages continuously...
        nbytes = recv(broker_fd, buf, sizeof(buf), 0);
        if (nbytes == -1) {
            perror("recv msg");
        } else {
            buf[nbytes] = '\0';

            cmd = buf[0];
            if (cmd == SOH) {
                topic = &buf[1];
                msg = spilt_at(buf, STX);

                printf("[%s] %s\n", topic, msg);
            }
        }
    }
}
