/*
 * A socket of an old libzmq, driven over stdin, so the interop tests can
 * talk to the ZMTP 1.0 (libzmq 2.x) and ZMTP 2.0 (libzmq 3.x) protocols.
 * A process apart, as two libzmqs cannot share one: they export the same
 * symbols.
 *
 *   zmq-legacy-peer <type> <bind|connect> <endpoint>
 *                   [--identity <hex>] [--subscribe <hex>]...
 *
 * Prints "ready" once bound or connected, then reads one command a line:
 *
 *   send <hex> [<hex>...]   sends one message, a frame per argument, and
 *                           prints "sent"
 *   recv                    prints "msg <hex> [<hex>...]" for the next
 *                           message, or "timeout" after five seconds
 *   quit                    closes the socket and exits
 *
 * An empty frame is "-". Anything that fails prints "error <what>" and
 * exits with status 1.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zmq.h>

#define MAX_FRAMES 64
#define MAX_FRAME 65536

static void fail (const char *what)
{
    printf ("error %s: %s\n", what, zmq_strerror (zmq_errno ()));
    fflush (stdout);
    exit (1);
}

static int socket_type (const char *name)
{
    static const struct {
        const char *name;
        int type;
    } types[] = {{"PAIR", 0},   {"PUB", 1},  {"SUB", 2},   {"REQ", 3},
                 {"REP", 4},    {"DEALER", 5}, {"ROUTER", 6}, {"PULL", 7},
                 {"PUSH", 8},   {"XPUB", 9}, {"XSUB", 10}};
    size_t i;
    for (i = 0; i < sizeof types / sizeof types[0]; i++)
        if (strcmp (types[i].name, name) == 0)
            return types[i].type;
    printf ("error unknown socket type %s\n", name);
    exit (1);
}

/* Decodes `hex` into `out`, returning the length; "-" is empty. */
static size_t unhex (const char *hex, unsigned char *out)
{
    size_t len = 0;
    if (strcmp (hex, "-") == 0)
        return 0;
    while (hex[0] && hex[1] && len < MAX_FRAME) {
        unsigned int octet;
        if (sscanf (hex, "%2x", &octet) != 1) {
            printf ("error bad hex %s\n", hex);
            exit (1);
        }
        out[len++] = (unsigned char) octet;
        hex += 2;
    }
    return len;
}

static void print_hex (const unsigned char *data, size_t len)
{
    size_t i;
    if (len == 0) {
        fputs ("-", stdout);
        return;
    }
    for (i = 0; i < len; i++)
        printf ("%02x", data[i]);
}

static void send_frame (void *socket, const unsigned char *data, size_t len, int more)
{
    zmq_msg_t msg;
    if (zmq_msg_init_size (&msg, len) != 0)
        fail ("zmq_msg_init_size");
    memcpy (zmq_msg_data (&msg), data, len);
#if ZMQ_VERSION_MAJOR == 2
    if (zmq_send (socket, &msg, more ? ZMQ_SNDMORE : 0) != 0)
        fail ("zmq_send");
#else
    if (zmq_msg_send (&msg, socket, more ? ZMQ_SNDMORE : 0) < 0)
        fail ("zmq_msg_send");
#endif
    zmq_msg_close (&msg);
}

/* Receives one frame and prints `prefix` and the frame; returns whether
   more follow, or -1 on timeout, having printed nothing. */
static int recv_frame (void *socket, const char *prefix)
{
    zmq_msg_t msg;
#if ZMQ_VERSION_MAJOR == 2
    int64_t more = 0;
#else
    int more = 0;
#endif
    size_t more_size = sizeof more;
    zmq_msg_init (&msg);
#if ZMQ_VERSION_MAJOR == 2
    if (zmq_recv (socket, &msg, 0) != 0) {
#else
    if (zmq_msg_recv (&msg, socket, 0) < 0) {
#endif
        if (zmq_errno () == EAGAIN)
            return -1;
        fail ("receive");
    }
    fputs (prefix, stdout);
    print_hex ((const unsigned char *) zmq_msg_data (&msg), zmq_msg_size (&msg));
    zmq_msg_close (&msg);
    if (zmq_getsockopt (socket, ZMQ_RCVMORE, &more, &more_size) != 0)
        fail ("ZMQ_RCVMORE");
    return more != 0;
}

int main (int argc, char *argv[])
{
    static unsigned char frames[MAX_FRAMES][MAX_FRAME];
    static char line[MAX_FRAMES * (2 * MAX_FRAME + 1) + 16];
    void *context;
    void *socket;
    int timeout = 5000;
    int linger = 0;
    int i;

    if (argc < 4) {
        fprintf (stderr, "usage: %s <type> <bind|connect> <endpoint> "
                         "[--identity <hex>] [--subscribe <hex>]...\n",
                 argv[0]);
        return 2;
    }
#if ZMQ_VERSION_MAJOR == 2
    context = zmq_init (1);
#else
    context = zmq_ctx_new ();
#endif
    if (!context)
        fail ("context");
    socket = zmq_socket (context, socket_type (argv[1]));
    if (!socket)
        fail ("zmq_socket");
    if (zmq_setsockopt (socket, ZMQ_RCVTIMEO, &timeout, sizeof timeout) != 0)
        fail ("ZMQ_RCVTIMEO");
    if (zmq_setsockopt (socket, ZMQ_LINGER, &linger, sizeof linger) != 0)
        fail ("ZMQ_LINGER");
    for (i = 4; i + 1 < argc; i += 2) {
        size_t len = unhex (argv[i + 1], frames[0]);
        int option;
        if (strcmp (argv[i], "--identity") == 0)
            option = ZMQ_IDENTITY;
        else if (strcmp (argv[i], "--subscribe") == 0)
            option = ZMQ_SUBSCRIBE;
        else {
            printf ("error unknown option %s\n", argv[i]);
            return 1;
        }
        if (zmq_setsockopt (socket, option, frames[0], len) != 0)
            fail (argv[i]);
    }
    if (strcmp (argv[2], "bind") == 0) {
        if (zmq_bind (socket, argv[3]) != 0)
            fail ("zmq_bind");
    } else if (zmq_connect (socket, argv[3]) != 0)
        fail ("zmq_connect");
    puts ("ready");
    fflush (stdout);

    while (fgets (line, sizeof line, stdin)) {
        char *command = strtok (line, " \n");
        if (!command)
            continue;
        if (strcmp (command, "send") == 0) {
            size_t lens[MAX_FRAMES];
            int count = 0;
            char *hex;
            while ((hex = strtok (NULL, " \n")) && count < MAX_FRAMES) {
                lens[count] = unhex (hex, frames[count]);
                count++;
            }
            for (i = 0; i < count; i++)
                send_frame (socket, frames[i], lens[i], i + 1 < count);
            puts ("sent");
        } else if (strcmp (command, "recv") == 0) {
            int more = recv_frame (socket, "msg ");
            if (more < 0) {
                puts ("timeout");
                fflush (stdout);
                continue;
            }
            while (more)
                more = recv_frame (socket, " ");
            puts ("");
        } else if (strcmp (command, "quit") == 0) {
            break;
        } else {
            printf ("error unknown command %s\n", command);
            return 1;
        }
        fflush (stdout);
    }
    zmq_close (socket);
#if ZMQ_VERSION_MAJOR == 2
    zmq_term (context);
#else
    zmq_ctx_destroy (context);
#endif
    return 0;
}
