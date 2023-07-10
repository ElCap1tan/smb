#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>

#define SERVER_PORT 8080
#define MSG_BUF_SIZE 2048
#define MAX_SUBSCRIBERS 512
#define MAX_TOPIC_LEN 512
#define TOPIC_SEPARATOR '/'
#define WILD_CARD "#"
#define SOH '\x01'
#define STX '\x02'

struct subscription {
    struct in_addr subscriber;
    uint16_t port;
    char topic[MAX_TOPIC_LEN];
    char subtopic[MAX_TOPIC_LEN];
} sub_list[MAX_SUBSCRIBERS];

char *spilt_at(char *str, char sep) {
    char *sep_ptr = strchr(str, sep);
    if (!sep_ptr) return sep_ptr;
    sep_ptr[0] = 0;
    return sep_ptr + 1;
}

int main() {
    char rcv_buf[MSG_BUF_SIZE], send_buf[MSG_BUF_SIZE], cmd, *msg_ptr, *topic, *subtopic;
    int sub_c = 0;
    int server_fd;
    struct sockaddr_in server_addr, client_addr;
    int errcode;
    uint addr_length;
    uint32_t msg_len;
    ssize_t nbytes;

    server_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (server_fd < 0) {
        perror("vlftpd: Error creating socket");
        return EXIT_FAILURE;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    memset(&client_addr, 0, sizeof(client_addr));
    
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(SERVER_PORT);

    errcode = bind(server_fd, (const struct sockaddr *) &server_addr, sizeof(server_addr));
    if (errcode < 0) {
        perror("vlftpd: Failed to bind socket");
        return EXIT_FAILURE;
    }

    printf("smbbroker: Listening on port %d\n", SERVER_PORT);

    addr_length = sizeof(client_addr);
    while(1) {
        memset(&client_addr, 0, sizeof(client_addr));
        nbytes = recvfrom(server_fd, rcv_buf, sizeof(rcv_buf), 0, (struct sockaddr *) &client_addr, &addr_length);
        if (nbytes == -1) {
            perror("recvfrom()");
            continue;
        }
        rcv_buf[nbytes] = '\0';

        cmd = rcv_buf[0];
        msg_ptr = &rcv_buf[1];

        switch (cmd) {
            case 's': {
                struct subscription *sub;
                sub = &sub_list[sub_c++];
                sub->subscriber = client_addr.sin_addr;
                sub->port = ntohs(client_addr.sin_port);

                topic = msg_ptr;
                if (!(subtopic = spilt_at(msg_ptr, TOPIC_SEPARATOR))) {
                    subtopic = "#";
                }

                strcpy(sub->topic, topic);
                strcpy(sub->subtopic, subtopic);
                printf("smbbroker: Topic '%s%c%s' added to subscription list for new subscriber %s:%d\n", msg_ptr, TOPIC_SEPARATOR, subtopic, inet_ntoa(sub->subscriber), sub->port);
                break;
            }
            case SOH: {
                char *msg;
                topic = msg_ptr;
                msg = spilt_at(msg_ptr, STX);
                subtopic = spilt_at(topic, TOPIC_SEPARATOR);

                printf("smbbroker: Received publish request for message '%s' on topic '%s%c%s' from %s:%d\n", msg, topic, TOPIC_SEPARATOR, subtopic, inet_ntoa(client_addr.sin_addr),
                       ntohs(client_addr.sin_port));

                for (int s = 0; s < sub_c; ++s) {
                    struct subscription *sub = &sub_list[s];

                    if (strcmp(sub->topic, topic) == 0 || strcmp(sub->topic, WILD_CARD) == 0) {
                        if (strcmp(sub->subtopic, subtopic) == 0 || strcmp(sub->subtopic, WILD_CARD) == 0) {
                            memset(&client_addr, 0, sizeof(client_addr));
                            client_addr.sin_family = AF_INET;
                            client_addr.sin_addr.s_addr = sub->subscriber.s_addr;
                            client_addr.sin_port = htons(sub->port);

                            printf("smbbroker: Relaying message '%s' on topic '%s%c%s' to %s:%d\n", msg, topic, TOPIC_SEPARATOR, subtopic, inet_ntoa(sub->subscriber), sub->port);
                            sprintf(send_buf, "%s%c%s%c%s", topic, TOPIC_SEPARATOR, subtopic, STX, msg);
                            msg_len = strlen(send_buf);
                            nbytes = sendto(server_fd, send_buf, msg_len, 0, (struct sockaddr *) &client_addr, sizeof(client_addr));
                            if (nbytes == -1) {
                                perror("smbbroker: sendto");
                            } else if (nbytes != msg_len) {
                                printf("smbbroker: Failed to relay message '%s' on topic '%s%c%s' to %s:%d\n", msg, topic, TOPIC_SEPARATOR, subtopic, inet_ntoa(sub->subscriber), sub->port);
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
    return EXIT_SUCCESS;
}
