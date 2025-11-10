#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 9999
DELIM = b'*'
BUFFER_SIZE = 4096

def run_client():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    print("Connected to Gateway.")
    buffer = b''
    try:
        while True:
            data = s.recv(BUFFER_SIZE)
            if not data:
                print("Connection closed by server.")
                break
            buffer += data
            # split by delimiter
            while True:
                idx = buffer.find(DELIM)
                if idx == -1:
                    break
                chunk = buffer[:idx]  # bytes before delimiter
                buffer = buffer[idx+1:]
                # parse chunk
                try:
                    text = chunk.decode('utf-8')
                except Exception:
                    text = repr(chunk)
                # chunk format: SYMBOL,PRICE  or NEWS,SENTIMENT
                print("MSG:", text)
    except KeyboardInterrupt:
        print("\nClient exiting.")
    finally:
        s.close()

if __name__ == "__main__":
    run_client()