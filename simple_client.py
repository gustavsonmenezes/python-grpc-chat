import grpc
import threading
import time
from datetime import datetime
import chat_pb2
import chat_pb2_grpc

# Cores para o terminal
class Colors:
    YOU = "\033[96m"     # Ciano
    OTHER = "\033[95m"   # Magenta
    SYSTEM = "\033[93m"  # Amarelo
    INPUT = "\033[92m"   # Verde
    ERROR = "\033[91m"   # Vermelho
    END = "\033[0m"      # Reset

def receive_messages(stub, user_name):
    """Função para receber mensagens em background"""
    try:
        print(f"{Colors.SYSTEM}📥 Ouvindo mensagens do servidor...{Colors.END}")
        
        for message in stub.ReceiveMessages(chat_pb2.Empty()):
            if message.user != user_name:  # Não mostra suas próprias mensagens
                timestamp = datetime.now().strftime("%H:%M:%S")
                print(f"\n{Colors.OTHER}┌─[{timestamp}]─ NOVA MENSAGEM ─┐")
                print(f"├─ De: {message.user}")
                print(f"├─ Conteúdo: {message.text}")
                print(f"└─────────────────────────────{Colors.END}")
                print(f"{Colors.INPUT}Sua mensagem > {Colors.END}", end='')
                
    except Exception as e:
        print(f"{Colors.ERROR}❌ Erro ao receber mensagens: {e}{Colors.END}")

def run():
    print(f"{Colors.SYSTEM}╔══════════════════════════════════════╗")
    print(f"║           CLIENTE gRPC CHAT          ║")
    print(f"╚══════════════════════════════════════╝{Colors.END}")
    
    # Configuração da conexão
    server_address = "localhost:50051"
    print(f"{Colors.SYSTEM}🔗 Conectando ao servidor: {server_address}{Colors.END}")
    
    try:
        channel = grpc.insecure_channel(server_address)
        stub = chat_pb2_grpc.SimpleChatStub(channel)
        
        # Testa a conexão
        response = stub.SendMessage(chat_pb2.ChatMessage(
            user="Sistema",
            text="Teste de conexão"
        ))
        
        print(f"{Colors.SYSTEM}✅ Conectado com sucesso!{Colors.END}")
        
    except Exception as e:
        print(f"{Colors.ERROR}❌ Erro ao conectar: {e}{Colors.END}")
        print(f"{Colors.ERROR}💡 Verifique se o servidor está rodando{Colors.END}")
        return
    
    # Nome do usuário
    user_name = input(f"{Colors.INPUT}👤 Digite seu nome: {Colors.END}").strip()
    if not user_name:
        user_name = "Anônimo"
    
    print(f"{Colors.SYSTEM}👋 Olá {user_name}! Você entrou no chat.{Colors.END}")
    print(f"{Colors.SYSTEM}💡 Comandos:")
    print(f"   - Digite sua mensagem e pressione Enter")
    print(f"   - Digite 'sair' para sair")
    print(f"   - Digite 'usuarios' para ver informações{Colors.END}")
    print(f"{Colors.SYSTEM}──────────────────────────────────────────{Colors.END}")
    
    # Inicia thread para receber mensagens
    receiver_thread = threading.Thread(
        target=receive_messages, 
        args=(stub, user_name),
        daemon=True
    )
    receiver_thread.start()
    
    # Loop principal para enviar mensagens
    message_count = 0
    while True:
        try:
            text = input(f"{Colors.INPUT}Sua mensagem > {Colors.END}").strip()
            
            if text.lower() == 'sair':
                print(f"{Colors.SYSTEM}👋 Saindo do chat...{Colors.END}")
                break
            elif text.lower() == 'usuarios':
                print(f"{Colors.SYSTEM}ℹ️  Você é: {user_name}")
                print(f"ℹ️  Mensagens enviadas: {message_count}{Colors.END}")
                continue
            elif not text:
                continue
            
            # Envia a mensagem
            response = stub.SendMessage(chat_pb2.ChatMessage(
                user=user_name,
                text=text
            ))
            
            message_count += 1
            timestamp = datetime.now().strftime("%H:%M:%S")
            
            if response.success:
                print(f"{Colors.YOU}✓ [{timestamp}] Sua mensagem foi enviada! ({message_count}ª){Colors.END}")
            else:
                print(f"{Colors.ERROR}✗ Erro ao enviar mensagem{Colors.END}")
                
        except KeyboardInterrupt:
            print(f"\n{Colors.SYSTEM}👋 Saindo do chat...{Colors.END}")
            break
        except Exception as e:
            print(f"{Colors.ERROR}❌ Erro: {e}{Colors.END}")
            break

if __name__ == '__main__':
    run()