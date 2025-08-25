import sys
import time
import queue
import threading
import grpc

import chat_pb2
import chat_pb2_grpc


def generate_requests(username: str, send_queue: 'queue.Queue[str]'):
    # Send a join message (optional, shown to others)
    yield chat_pb2.ChatMessage(user=username, message='joined the chat', timestamp=int(time.time() * 1000))
    while True:
        msg = send_queue.get()
        if msg is None:  # sentinel for shutdown
            break
        yield chat_pb2.ChatMessage(user=username, message=msg, timestamp=int(time.time() * 1000))


essage_prompt = "Type a message (/quit to exit): "

def input_worker(send_queue: 'queue.Queue[str]'):
    try:
        while True:
            text = input(message_prompt).strip()
            if text.lower() in {"/quit", "/exit"}:
                send_queue.put(None)
                break
            if text:
                send_queue.put(text)
    except (KeyboardInterrupt, EOFError):
        send_queue.put(None)


def run():
    username = input("Enter your name: ").strip() or "Anonymous"

    with grpc.insecure_channel('localhost:50051') as channel:
        stub = chat_pb2_grpc.ChatServiceStub(channel)
        send_queue: 'queue.Queue[str]' = queue.Queue()

        # Start input thread
        threading.Thread(target=input_worker, args=(send_queue,), daemon=True).start()

        # Start the chat stream
        responses = stub.Chat(generate_requests(username, send_queue))

        try:
            for res in responses:
                # Print incoming messages; avoid duplicating our own join line
                ts = time.strftime('%H:%M:%S', time.localtime(res.timestamp / 1000)) if res.timestamp else ''
                print(f"\n[{ts}] {res.user}: {res.message}")
        except grpc.RpcError as e:
            print("Disconnected:", e)


if __name__ == '__main__':
    run()
