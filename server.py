import grpc
from concurrent import futures
import threading
import time
import logging
from datetime import datetime

import chat_pb2
import chat_pb2_grpc
import naming_service_pb2
import naming_service_pb2_grpc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ChatServer(chat_pb2_grpc.ChatServiceServicer):
    def __init__(self, server_id, port, naming_server_address='localhost:50051'):
        self.server_id = server_id
        self.port = port
        self.address = f'localhost:{port}'
        self.naming_server_address = naming_server_address
        
        self.clients = {}  # {username: (address, stream)}
        self.messages = []
        self.lock = threading.Lock()
        self.is_primary = True
        self.running = True
        
        # Heartbeat configuration
        self.heartbeat_interval = 10  # seconds
        self.heartbeat_thread = None
        
        # Register with naming service
        self._register_with_naming_service()
        
        # Start heartbeat thread
        self._start_heartbeat()
    
    def _register_with_naming_service(self):
        try:
            channel = grpc.insecure_channel(self.naming_server_address)
            stub = naming_service_pb2_grpc.NamingServiceStub(channel)
            
            response = stub.RegisterServer(naming_service_pb2.RegisterRequest(
                server_id=self.server_id,
                address=self.address,
                is_primary=True
            ))
            
            if response.success:
                logger.info(f"Servidor {self.server_id} registrado como primário no Serviço de Nomes")
            else:
                logger.error("Falha ao registrar servidor no Serviço de Nomes")
                
        except Exception as e:
            logger.error(f"Erro ao registrar no Serviço de Nomes: {e}")
    
    def _start_heartbeat(self):
        def send_heartbeat():
            while self.running:
                try:
                    channel = grpc.insecure_channel(self.naming_server_address)
                    stub = naming_service_pb2_grpc.NamingServiceStub(channel)
                    
                    response = stub.Heartbeat(naming_service_pb2.HeartbeatRequest(
                        server_id=self.server_id
                    ))
                    
                    if response.success:
                        logger.debug(f"Heartbeat enviado por {self.server_id}")
                    else:
                        logger.warning(f"Falha no heartbeat do servidor {self.server_id}")
                        
                except Exception as e:
                    logger.error(f"Erro ao enviar heartbeat: {e}")
                
                time.sleep(self.heartbeat_interval)
        
        self.heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
        self.heartbeat_thread.start()
        logger.info(f"Heartbeat iniciado para servidor {self.server_id}")
    
    def Connect(self, request, context):
        username = request.username
        
        with self.lock:
            if username in self.clients:
                return chat_pb2.ConnectResponse(success=False, message="Usuário já conectado")
            
            # For now, just store the username without stream
            self.clients[username] = (context.peer(), None)
        
        logger.info(f"Usuário {username} conectado")
        return chat_pb2.ConnectResponse(success=True, message="Conectado com sucesso")
    
    def SendMessage(self, request, context):
        username = request.username
        text = request.text
        timestamp = datetime.now().isoformat()
        
        with self.lock:
            if username not in self.clients:
                return chat_pb2.MessageResponse(success=False, message="Usuário não conectado")
            
            # Store message
            message_data = {
                'username': username,
                'text': text,
                'timestamp': timestamp
            }
            self.messages.append(message_data)
            
            # Broadcast to all connected clients (simplified)
            logger.info(f"Mensagem de {username}: {text}")
        
        return chat_pb2.MessageResponse(success=True, message="Mensagem enviada")
    
    def GetMessages(self, request, context):
        username = request.username
        
        with self.lock:
            if username not in self.clients:
                yield chat_pb2.ChatMessage(success=False, message="Usuário não conectado")
                return
            
            # Send all messages to client
            for msg in self.messages:
                yield chat_pb2.ChatMessage(
                    username=msg['username'],
                    text=msg['text'],
                    timestamp=msg['timestamp'],
                    success=True
                )
    
    def ListUsers(self, request, context):
        with self.lock:
            users = list(self.clients.keys())
        
        return chat_pb2.UserList(users=users, success=True)
    
    def Disconnect(self, request, context):
        username = request.username
        
        with self.lock:
            if username in self.clients:
                del self.clients[username]
                logger.info(f"Usuário {username} desconectado")
                return chat_pb2.DisconnectResponse(success=True, message="Desconectado")
            else:
                return chat_pb2.DisconnectResponse(success=False, message="Usuário não encontrado")
    
    def stop(self):
        self.running = False
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=5)

def serve(server_id='server_1', port=50052):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    chat_server = ChatServer(server_id, port)
    chat_pb2_grpc.add_ChatServiceServicer_to_server(chat_server, server)
    
    server_address = f'[::]:{port}'
    server.add_insecure_port(server_address)
    server.start()
    logger.info(f"Servidor de Chat {server_id} iniciado em {server_address}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info(f"Desligando servidor {server_id}...")
        chat_server.stop()

if __name__ == '__main__':
    import sys
    server_id = sys.argv[1] if len(sys.argv) > 1 else 'server_1'
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 50052
    serve(server_id, port)