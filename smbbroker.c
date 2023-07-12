/**
 * smbbroker.c
 * Simple message broker that listens for publish requests and relays the messages to it's subscribers.
 */

#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>

#define SERVER_PORT 8080
#define MSG_BUF_SIZE 4096
#define MAX_SUBSCRIBERS 512
#define MAX_TOPIC_LEN 512
#define ACK 'A'                 // Used as the start of an ACKNOWLEDGE message
#define SUB 'S'                 // Used as the start of a SUBSCRIBE message
#define SOH '\x01'              // Start of heading control char: Used to start a publish request message
#define STX '\x02'              // Start of text control char: Used to separate topic and message
#define TOPIC_SEPARATOR '/'     // Used to separate topic and subtopic
#define WILD_CARD "#"

// Struct represents a subscription of a single client
struct subscription {
    struct in_addr sub_addr;            // IP address of client
    uint16_t port;                      // Port of client
    char topic[MAX_TOPIC_LEN + 1];      // Topic subscribed to
    char subtopic[MAX_TOPIC_LEN + 1];   // Subtopic subscribed to
} sub_list[MAX_SUBSCRIBERS];            // List of subscribers

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

int main() {
    char rcv_buf[MSG_BUF_SIZE], send_buf[MSG_BUF_SIZE];
    char cmd, *msg_ptr, *topic, *subtopic;
    int sub_c = 0; // Counts the number of subscribed clients
    int broker_fd;
    struct sockaddr_in server_addr, client_addr;
    int errcode;
    uint addr_length;
    uint32_t msg_len;
    ssize_t nbytes;

    // Create broker socket
    broker_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (broker_fd < 0) {
        perror("vlftpd: Error creating socket");
        return EXIT_FAILURE;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    memset(&client_addr, 0, sizeof(client_addr));

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY; // Set the address struct to accepts connections from any address
    server_addr.sin_port = htons(SERVER_PORT);

    // Bind socket to port
    errcode = bind(broker_fd, (const struct sockaddr *) &server_addr, sizeof(server_addr));
    if (errcode < 0) {
        perror("smbbroker: Failed to bind socket");
        return EXIT_FAILURE;
    }

    printf("smbbroker: Listening on port %d\n", SERVER_PORT);

    addr_length = sizeof(client_addr);
    while(1) { // Continuously listen for subscribing or publish requests...
        memset(&client_addr, 0, sizeof(client_addr));
        nbytes = recvfrom(broker_fd, rcv_buf, sizeof(rcv_buf), 0, (struct sockaddr *) &client_addr, &addr_length);
        if (nbytes == -1) {
            perror("recvfrom");
            continue;
        }
        rcv_buf[nbytes] = '\0';

        cmd = rcv_buf[0];
        msg_ptr = &rcv_buf[1];

        switch (cmd) {
            case SUB: { // SUBSCRIPTION request
                struct subscription *sub;
                uint8_t exists = 0;

                // Go through the subscription list to check if the client of current request already is in the list.
                for (int i = 0; i < sub_c; ++i) {
                    sub = &sub_list[i];
                    if (sub->sub_addr.s_addr == client_addr.sin_addr.s_addr && sub->port == ntohs(client_addr.sin_port)) {
                        exists = 1;
                        break;
                    }
                }

                // If the client is not in the list, add it.
                if (!exists) {
                    sub = &sub_list[sub_c++];
                    sub->sub_addr = client_addr.sin_addr;
                    sub->port = ntohs(client_addr.sin_port);

                    topic = msg_ptr;
                    if (!(subtopic = spilt_at(msg_ptr, TOPIC_SEPARATOR))) {
                        subtopic = "#";
                    }

                    snprintf(sub->topic, sizeof(sub->topic), "%s", topic);
                    snprintf(sub->subtopic, sizeof(sub->subtopic), "%s", subtopic);
                    printf("smbbroker: Topic '%s%c%s' added to subscription list for new subscriber %s:%d\n", msg_ptr, TOPIC_SEPARATOR, subtopic, inet_ntoa(sub->sub_addr), sub->port);
                } else {
                    printf("smbbroker: Subscriber %s:%d already in subscription list with topic '%s%c%s'. Sending acknowledge again...\n", inet_ntoa(sub->sub_addr), sub->port, sub->topic, TOPIC_SEPARATOR, sub->subtopic);
                }

                // In any case, we send an acknowledgement message to the client.
                snprintf(send_buf, sizeof(send_buf), "%c%s%c%s", ACK, sub->topic, TOPIC_SEPARATOR, sub->subtopic);
                msg_len = strlen(send_buf);

                nbytes = sendto(broker_fd, send_buf, strlen(send_buf), 0, (struct sockaddr *) &client_addr, sizeof(client_addr));
                if (nbytes == -1) {
                    perror("smbbroker: sendto acknowledge");
                } else if (nbytes != msg_len) {
                    printf("smbbroker: Failed to send acknowledge to %s:%d\n", inet_ntoa(sub->sub_addr), sub->port);
                } else {
                    printf("smbbroker: Acknowledge send to %s:%d\n", inet_ntoa(sub->sub_addr), sub->port);
                }
                break;
            }
            case SOH: { // PUBLISH request
                char *msg;
                topic = msg_ptr;
                msg = spilt_at(msg_ptr, STX);
                subtopic = spilt_at(topic, TOPIC_SEPARATOR);

                printf("smbbroker: Received publish request for message '%s' on topic '%s%c%s' from %s:%d\n", msg, topic, TOPIC_SEPARATOR, subtopic, inet_ntoa(client_addr.sin_addr),
                       ntohs(client_addr.sin_port));

                // Go through the subscription list...
                for (int s = 0; s < sub_c; ++s) {
                    struct subscription *sub = &sub_list[s];

                    // ...and check if the topic matches the topic of the PUBLISH request (or is the wildcard).
                    if (strcmp(sub->topic, topic) == 0 || strcmp(sub->topic, WILD_CARD) == 0) {
                        // Check if it also matches the subtopic (or is the wildcard).
                        if (strcmp(sub->subtopic, subtopic) == 0 || strcmp(sub->subtopic, WILD_CARD) == 0) {
                            // If both matches, we relay the message to the corresponding client.
                            memset(&client_addr, 0, sizeof(client_addr));
                            client_addr.sin_family = AF_INET;
                            client_addr.sin_addr = sub->sub_addr;
                            client_addr.sin_port = htons(sub->port);

                            printf("smbbroker: Relaying message '%s' on topic '%s%c%s' to %s:%d\n", msg, topic, TOPIC_SEPARATOR, subtopic, inet_ntoa(sub->sub_addr), sub->port);
                            snprintf(send_buf, sizeof(send_buf), "%c%s%c%s%c%s", SOH, topic, TOPIC_SEPARATOR, subtopic, STX, msg);
                            msg_len = strlen(send_buf);
                            nbytes = sendto(broker_fd, send_buf, msg_len, 0, (struct sockaddr *) &client_addr, sizeof(client_addr));
                            if (nbytes == -1) {
                                perror("smbbroker: sendto");
                            } else if (nbytes != msg_len) {
                                printf("smbbroker: Failed to relay message '%s' on topic '%s%c%s' to %s:%d\n", msg, topic, TOPIC_SEPARATOR, subtopic, inet_ntoa(sub->sub_addr), sub->port);
                            }
                        }
                    }
                }
                break;
            }
            default: {
                printf("smbbroker: Received unknown command: %c\n", cmd);
                break;
            }
        }
    }
}
