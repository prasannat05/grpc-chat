import grpc
from concurrent import futures
import time
import threading
import queue
import datetime

import chat_pb2
import chat_pb2_grpc

class ChatService(chat_pb2_grpc.ChatServiceServicer):
    def __init__(self):
        self.clients = []  # list[queue.Queue]
        self.lock = threading.Lock()

    def broadcast(self, msg: chat_pb2.ChatMessage):
        with self.lock:
            for q in list(self.clients):
                # Non-blocking put with fallback
                try:
                    q.put_nowait(msg)
                except queue.Full:
                    # Drop if the client's queue is full to avoid server blocking
                    pass

    def Chat(self, request_iterator, context):
        client_queue: queue.Queue[chat_pb2.ChatMessage] = queue.Queue(maxsize=100)
        with self.lock:
            self.clients.append(client_queue)

        # Producer: read messages from this client's stream and broadcast
        def reader():
            try:
                for msg in request_iterator:
                    # Ensure timestamp is set if client didn't send it
                    ts = msg.timestamp or int(time.time() * 1000)
                    enriched = chat_pb2.ChatMessage(user=msg.user, message=msg.message, timestamp=ts)
                    print(f"[{enriched.user}] {enriched.message}")
                    self.broadcast(enriched)
            except Exception:
                # Stream ended or error; just exit reader thread
                pass

        t = threading.Thread(target=reader, daemon=True)
        t.start()

        try:
            # Consumer: yield messages destined for this client
            while True:
                msg = client_queue.get()
                yield msg
        except Exception:
            pass
        finally:
            with self.lock:
                if client_queue in self.clients:
                    self.clients.remove(client_queue)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    chat_pb2_grpc.add_ChatServiceServicer_to_server(ChatService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print('🚀 Chat server started on port 50051')
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print('\nShutting down...')
        server.stop(grace=None)


if __name__ == '__main__':
    serve()
