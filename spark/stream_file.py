import socket
import time
from math import trunc


# Acts as a server, streaming the txt file content to an open socket
def send_file_over_socket(file_path, host='localhost', port=9999, delay=0.5):
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

    # finally:
    #     # Close the sockets
    #     client_socket.close()
    #     server_socket.close()
    #     print("Connection closed.")


if __name__ == "__main__":
    file_path = "/home/bigdata/PycharmProjects/SparkStreamingCotiles/spark/NetworkSegmentETiles30.txt"
    # file_path = "/home/bigdata/master/MEDES/NetworkSegment_small.txt" smaller file for testing
    send_file_over_socket(file_path)