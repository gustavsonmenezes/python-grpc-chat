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

class BackupServer(chat_pb2_grpc.ChatServiceServicer):
    def __init__(self, server_id, port, primary_server_address, naming_server_address='localhost:50051'):
        self.server_id = server_id
        self.port = port
        self.address = f'localhost:{port}'
        self.primary_server_address = primary_server_address
        self.naming_server_address = naming_server_address
        
        self.clients = {}
        self.messages = []
        self.lock = threading.Lock()
        self.is_primary = False
        self.running = True
        
        # Heartbeat monitoring
        self.heartbeat_timeout = 30  # seconds
        self.last_heartbeat_time = None
        self.monitoring_thread = None
        
        # Register as backup
        self._register_with_naming_service()
        
        # Start monitoring primary server
        self._start_primary_monitoring()
    
    def _register_with_naming_service(self):
        try:
            channel = grpc.insecure_channel(self.naming_server_address)
            stub = naming_service_pb2_grpc.NamingServiceStub(channel)
            
            response = stub.RegisterServer(naming_service_pb2.RegisterRequest(
                server_id=self.server_id,
                address=self.address,
                is_primary=False  # Starts as backup
            ))
            
            if response.success:
                logger.info(f"Servidor {self.server_id} registrado como backup no Serviço de Nomes")
            else:
                logger.error("Falha ao registrar servidor backup no Serviço de Nomes")
                
        except Exception as e:
            logger.error(f"Erro ao registrar backup no Serviço de Nomes: {e}")
    
    def _start_primary_monitoring(self):
        def monitor_primary():
            while self.running and not self.is_primary:
                try:
                    # Check primary server status through naming service
                    channel = grpc.insecure_channel(self.naming_server_address)
                    stub = naming_service_pb2_grpc.NamingServiceStub(channel)
                    
                    response = stub.LookupServer(naming_service_pb2.LookupRequest())
                    
                    if not response.success:
                        logger.warning("Nenhum servidor primário ativo encontrado. Promovendo backup...")
                        self._promote_to_primary()
                    
                except Exception as e:
                    logger.error(f"Erro ao monitorar servidor primário: {e}")
                    # If we can't contact naming service, assume primary is down
                    self._promote_to_primary()
                
                time.sleep(10)  # Check every 10 seconds
        
        self.monitoring_thread = threading.Thread(target=monitor_primary, daemon=True)
        self.monitoring_thread.start()
        logger.info(f"Monitoramento do servidor primário iniciado")
    
    def _promote_to_primary(self):
        """Promote this backup server to primary"""
        try:
            with self.lock:
                self.is_primary = True
            
            # Re-register as primary with naming service
            channel = grpc.insecure_channel(self.naming_server_address)
            stub = naming_service_pb2_grpc.NamingServiceStub(channel)
            
            response = stub.RegisterServer(naming_service_pb2.RegisterRequest(
                server_id=self.server_id,
                address=self.address,
                is_primary=True
            ))
            
            if response.success:
                logger.info(f"✅ Servidor {self.server_id} promovido a PRIMÁRIO")
                
                # Start sending heartbeats as primary
                self._start_heartbeat()
            else:
                logger.error("Falha ao promover servidor a primário")
                
        except Exception as e:
            logger.error(f"Erro ao promover servidor: {e}")
    
    def _start_heartbeat(self):
        def send_heartbeat():
            while self.running and self.is_primary:
                try:
                    channel = grpc.insecure_channel(self.naming_server_address)
                    stub = naming_service_pb2_grpc.NamingServiceStub(channel)
                    
                    response = stub.Heartbeat(naming_service_pb2.HeartbeatRequest(
                        server_id=self.server_id
                    ))
                    
                    if response.success:
                        logger.debug(f"Heartbeat enviado por {self.server_id} (primário)")
                    else:
                        logger.warning(f"Falha no heartbeat do servidor primário {self.server_id}")
                        
                except Exception as e:
                    logger.error(f"Erro ao enviar heartbeat: {e}")
                
                time.sleep(10)  # Heartbeat every 10 seconds
        
        heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
        heartbeat_thread.start()
        logger.info(f"Heartbeat iniciado para servidor primário {self.server_id}")
    
    # Implement the same chat methods as primary server
    def Connect(self, request, context):
        if not self.is_primary:
            return chat_pb2.ConnectResponse(
                success=False, 
                message="Servidor em modo backup. Conecte-se ao servidor primário."
            )
        
        username = request.username
        
        with self.lock:
            if username in self.clients:
                return chat_pb2.ConnectResponse(success=False, message="Usuário já conectado")
            
            self.clients[username] = (context.peer(), None)
        
        logger.info(f"Usuário {username} conectado ao backup promovido")
        return chat_pb2.ConnectResponse(success=True, message="Conectado com sucesso")
    
    def SendMessage(self, request, context):
        if not self.is_primary:
            return chat_pb2.MessageResponse(
                success=False, 
                message="Servidor em modo backup. Operação não permitida."
            )
        
        username = request.username
        text = request.text
        timestamp = datetime.now().isoformat()
        
        with self.lock:
            if username not in self.clients:
                return chat_pb2.MessageResponse(success=False, message="Usuário não conectado")
            
            message_data = {
                'username': username,
                'text': text,
                'timestamp': timestamp
            }
            self.messages.append(message_data)
            
            logger.info(f"Mensagem de {username}: {text}")
        
        return chat_pb2.MessageResponse(success=True, message="Mensagem enviada")
    
    def GetMessages(self, request, context):
        if not self.is_primary:
            yield chat_pb2.ChatMessage(success=False, message="Servidor em modo backup")
            return
        
        username = request.username
        
        with self.lock:
            if username not in self.clients:
                yield chat_pb2.ChatMessage(success=False, message="Usuário não conectado")
                return
            
            for msg in self.messages:
                yield chat_pb2.ChatMessage(
                    username=msg['username'],
                    text=msg['text'],
                    timestamp=msg['timestamp'],
                    success=True
                )
    
    def ListUsers(self, request, context):
        if not self.is_primary:
            return chat_pb2.UserList(users=[], success=False)
        
        with self.lock:
            users = list(self.clients.keys())
        
        return chat_pb2.UserList(users=users, success=True)
    
    def Disconnect(self, request, context):
        if not self.is_primary:
            return chat_pb2.DisconnectResponse(
                success=False, 
                message="Servidor em modo backup"
            )
        
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

def serve(server_id='backup_1', port=50053, primary_address='localhost:50052'):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    backup_server = BackupServer(server_id, port, primary_address)
    chat_pb2_grpc.add_ChatServiceServicer_to_server(backup_server, server)
    
    server_address = f'[::]:{port}'
    server.add_insecure_port(server_address)
    server.start()
    logger.info(f"Servidor de Backup {server_id} iniciado em {server_address}")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info(f"Desligando servidor backup {server_id}...")
        backup_server.stop()

if __name__ == '__main__':
    import sys
    server_id = sys.argv[1] if len(sys.argv) > 1 else 'backup_1'
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 50053
    primary_address = sys.argv[3] if len(sys.argv) > 3 else 'localhost:50052'
    serve(server_id, port, primary_address)