/**
 * smbcontipublish.c
 * Simple message broker publisher that continuously publishes the time on topic 'time/germany'.
 */

#include <stdio.h>
#include <stdlib.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define SERVER_PORT 8080
#define MSG_BUF_SIZE 4096
#define SOH '\x01'              // Start of heading control char: Used to start a publish request message
#define STX '\x02'              // Start of text control char: Used to separate topic and message
#define TOPIC_SEPARATOR '/'     // Used to separate topic and subtopic
#define WILD_CARD "#"
#define INTERVAL_SEC 10         // Interval in which the messages are send

/**
 * Prints usage information
 */
void print_usage(char *argv[]) {
    printf("Usage: '%s broker'\n", argv[0]);
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
void validate_args(int argc, char *argv[], char **hostname) {
    if (argc == 1) {
        print_usage(argv);
        exit(EXIT_SUCCESS);
    }

    *hostname = argv[1];
}

/**
 * Returns string representation of the current local time.
 * @return A string representation of the current local time in the format "Day Mon dd hh:mm:ss yyyy"
 */
char *get_local_time_str() {
    time_t raw_time;
    struct tm * time_info;
    char *time_str;

    time(&raw_time);
    time_info = localtime(&raw_time);
    time_str = asctime(time_info);
    time_str[strlen(time_str) - 1] = 0;

    return time_str;
}

int main(int argc, char *argv[]) {
    char buf[MSG_BUF_SIZE];
    char *hostname;
    char *topic, *subtopic, *msg;
    struct sockaddr_in *broker_addr;
    int broker_fd, errcode;
    uint addr_length;
    ssize_t nbytes;

    validate_args(argc, argv, &hostname);

    topic = "time";
    subtopic = "germany";

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

    while (1) { // Continuously send time messages...
        msg = get_local_time_str();

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

        printf("[%s] Time published on topic '%s%c%s'...\n", msg, topic, TOPIC_SEPARATOR, subtopic);
        sleep(INTERVAL_SEC);
    }
}
