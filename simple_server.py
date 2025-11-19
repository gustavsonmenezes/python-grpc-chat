# simple_server.py
import grpc
from concurrent import futures
import time
from datetime import datetime
import chat_pb2
import chat_pb2_grpc

# Cores para o terminal
class Colors:
    SERVER = "\033[94m"  # Azul
    CLIENT = "\033[92m"  # Verde
    WARNING = "\033[93m" # Amarelo
    ERROR = "\033[91m"   # Vermelho
    END = "\033[0m"      # Reset

class SimpleChatServicer(chat_pb2_grpc.SimpleChatServicer):
    def __init__(self):
        self.messages = []
        self.connected_users = set()
        print(f"{Colors.SERVER}╔══════════════════════════════════════╗")
        print(f"║           SERVIDOR gRPC INICIADO       ║")
        print(f"║            Porto: 50051               ║")
        print(f"╚══════════════════════════════════════╝{Colors.END}")
    
    def SendMessage(self, request, context):
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        print(f"{Colors.CLIENT}┌─[{timestamp}]─ MENSAGEM RECEBIDA ─┐")
        print(f"├─ Usuário: {request.user}")
        print(f"├─ Mensagem: {request.text}")
        print(f"└─ De: {context.peer()}{Colors.END}")
        
        # Adiciona à lista de mensagens
        self.messages.append(request)
        self.connected_users.add(request.user)
        
        print(f"{Colors.SERVER}✓ Mensagem armazenada (Total: {len(self.messages)}){Colors.END}")
        
        return chat_pb2.ChatResponse(
            success=True,
            status=f"Mensagem entregue em {timestamp}"
        )
    
    def ReceiveMessages(self, request, context):
        peer_info = context.peer()
        print(f"{Colors.WARNING}📡 Nova conexão para receber mensagens: {peer_info}{Colors.END}")
        
        # Envia todas as mensagens para o cliente
        for i, message in enumerate(self.messages):
            print(f"{Colors.SERVER}↳ Enviando mensagem {i+1} para {peer_info}{Colors.END}")
            yield message

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    chat_pb2_grpc.add_SimpleChatServicer_to_server(SimpleChatServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    
    print(f"{Colors.SERVER}🎯 Servidor rodando! Aguardando conexões...{Colors.END}")
    print(f"{Colors.SERVER}💡 Dica: Abra outro terminal e execute o cliente{Colors.END}")
    print(f"{Colors.SERVER}⏹️  Pressione Ctrl+C para parar o servidor{Colors.END}")
    
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print(f"\n{Colors.WARNING}🛑 Parando servidor...{Colors.END}")
        server.stop(0)
        print(f"{Colors.ERROR}❌ Servidor parado.{Colors.END}")

if __name__ == '__main__':
    serve()