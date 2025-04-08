import os
import socket
import time
import argparse
from math import trunc


# Acts as a server, streaming the txt file content to an open socket
def send_file_over_socket(file_path, host='localhost', port=9999, delay=0.3):
    # Create a socket object
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(1)

    print(f"Listening on {host}:{port}... Waiting for a connection.")

    # Wait for a client to connect
    client_socket, client_address = server_socket.accept()
    print(f"Connection from {client_address} established.")


    while True:
        try:
            # Wait for a client to connect
            client_socket, client_address = server_socket.accept()
            print(f"Connection from {client_address} established.")

            try:
                # with client_socket:  # Use a context manager to handle the client socket
                while True:
                    with open(file_path, 'r') as file:
                        for line in file:
                            # Strip newline characters
                            data = line.strip()

                            if data:  # Send only non-empty lines
                                message = data + '\n'
                                try:
                                    print(f"Sending: {message}")
                                    client_socket.sendall(message.encode('utf-8'))
                                    # Add a delay to simulate streaming data
                                    time.sleep(delay)
                                except BrokenPipeError:
                                    print(f"Client disconnected while sending data.")
                                    break  # Exit the inner loop to handle client disconnection

                    # When the file has been fully read, print the message and start over
                    print("End of file reached. Restarting from the beginning...")
                    file.seek(0)  # Reset the file pointer to the beginning

            except (ConnectionResetError, ConnectionAbortedError):
                print(f"Client disconnected. Waiting for a new client...")
                # Continue to listen for new connections

        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    basePath = os.getcwd()
    file_path = f"{basePath}/Small_NetworkData2.txt"

    parser = argparse.ArgumentParser(description="Send a file over a socket.")
    parser.add_argument("--file_path", type=str, default=file_path, help="Path to the file to be sent over the socket.")
    parser.add_argument("--host", type=str, default="localhost", help="Host to bind the server (default: localhost).")
    parser.add_argument("--port", type=int, default=9999, help="Port to bind the server (default: 9999).")

    # file_path = "/home/bigdata/PycharmProjects/SparkStreamingCotiles/spark/NetworkSegmentETiles30.txt"
    # file_path = "/home/bigdata/PycharmProjects/SparkStreamingCotiles/spark/Small_NetworkData.txt"
    # file_path = "/home/bigdata/master/MEDES/NetworkSegment_small.txt"

    # Parse the arguments
    args = parser.parse_args()

    # Call the function with the parsed arguments
    send_file_over_socket(file_path=args.file_path, host=args.host, port=args.port)
