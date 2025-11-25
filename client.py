import grpc
import threading
import time
import logging

import chat_pb2
import chat_pb2_grpc
import naming_service_pb2
import naming_service_pb2_grpc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ChatClient:
    def __init__(self, naming_server_address='localhost:50051'):
        self.naming_server_address = naming_server_address
        self.server_address = None
        self.channel = None
        self.stub = None
        self.username = None
        self.connected = False
        self.reconnect_attempts = 3
        self.reconnect_delay = 5  # seconds
    
    def _discover_server(self):
        """Discover primary server through naming service"""
        try:
            channel = grpc.insecure_channel(self.naming_server_address)
            stub = naming_service_pb2_grpc.NamingServiceStub(channel)
            
            response = stub.LookupServer(naming_service_pb2.LookupRequest())
            
            if response.success:
                logger.info(f"Servidor encontrado: {response.server_id} ({response.address})")
                return response.address
            else:
                logger.error("Nenhum servidor ativo encontrado")
                return None
                
        except Exception as e:
            logger.error(f"Erro ao descobrir servidor: {e}")
            return None
    
    def _connect_to_server(self, server_address):
        """Connect to a specific server"""
        try:
            if self.channel:
                self.channel.close()
            
            self.channel = grpc.insecure_channel(server_address)
            self.stub = chat_pb2_grpc.ChatServiceStub(self.channel)
            self.server_address = server_address
            return True
            
        except Exception as e:
            logger.error(f"Erro ao conectar com servidor {server_address}: {e}")
            return False
    
    def connect(self, username):
        """Connect to chat with automatic failover"""
        self.username = username
        
        for attempt in range(self.reconnect_attempts):
            server_address = self._discover_server()
            
            if not server_address:
                logger.error("Não foi possível encontrar um servidor")
                time.sleep(self.reconnect_delay)
                continue
            
            if self._connect_to_server(server_address):
                try:
                    response = self.stub.Connect(chat_pb2.ConnectRequest(username=username))
                    
                    if response.success:
                        self.connected = True
                        logger.info(f"Conectado como {username} no servidor {self.server_address}")
                        return True
                    else:
                        logger.error(f"Falha na conexão: {response.message}")
                        
                except grpc.RpcError as e:
                    logger.warning(f"Erro ao conectar com servidor {server_address}: {e}")
            
            logger.info(f"Tentativa {attempt + 1}/{self.reconnect_attempts} falhou. Tentando novamente...")
            time.sleep(self.reconnect_delay)
        
        logger.error("Não foi possível conectar após várias tentativas")
        return False
    
    def send_message(self, text):
        if not self.connected:
            logger.error("Cliente não conectado")
            return False
        
        try:
            response = self.stub.SendMessage(chat_pb2.MessageRequest(
                username=self.username,
                text=text
            ))
            
            if response.success:
                logger.info("Mensagem enviada")
                return True
            else:
                logger.error(f"Erro ao enviar mensagem: {response.message}")
                return False
                
        except grpc.RpcError as e:
            logger.error(f"Erro de comunicação: {e}")
            self.connected = False
            return False
    
    def receive_messages(self):
        if not self.connected:
            return
        
        try:
            for message in self.stub.GetMessages(chat_pb2.GetMessagesRequest(username=self.username)):
                if message.success:
                    print(f"[{message.timestamp}] {message.username}: {message.text}")
                else:
                    logger.error(f"Erro: {message.message}")
                    
        except grpc.RpcError as e:
            logger.error(f"Erro ao receber mensagens: {e}")
            self.connected = False
    
    def list_users(self):
        if not self.connected:
            logger.error("Cliente não conectado")
            return []
        
        try:
            response = self.stub.ListUsers(chat_pb2.ListUsersRequest())
            
            if response.success:
                logger.info(f"Usuários conectados: {', '.join(response.users)}")
                return response.users
            else:
                logger.error("Erro ao listar usuários")
                return []
                
        except grpc.RpcError as e:
            logger.error(f"Erro de comunicação: {e}")
            self.connected = False
            return []
    
    def disconnect(self):
        if self.connected and self.stub:
            try:
                response = self.stub.Disconnect(chat_pb2.DisconnectRequest(username=self.username))
                if response.success:
                    logger.info("Desconectado com sucesso")
            except grpc.RpcError:
                pass  # Server might be down
        
        if self.channel:
            self.channel.close()
        
        self.connected = False

def main():
    client = ChatClient()
    
    username = input("Digite seu nome de usuário: ")
    
    if not client.connect(username):
        return
    
    # Start message receiving in background thread
    receive_thread = threading.Thread(target=client.receive_messages, daemon=True)
    receive_thread.start()
    
    try:
        while client.connected:
            message = input()
            
            if message.lower() == '/quit':
                break
            elif message.lower() == '/users':
                client.list_users()
            elif message.lower() == '/reconnect':
                client.disconnect()
                client.connect(username)
            else:
                if not client.send_message(message):
                    # Try to reconnect
                    logger.info("Tentando reconectar...")
                    if client.connect(username):
                        client.send_message(message)
                    else:
                        logger.error("Não foi possível reconectar")
                        break
    
    except KeyboardInterrupt:
        logger.info("Desconectando...")
    
    finally:
        client.disconnect()

if __name__ == '__main__':
    main()