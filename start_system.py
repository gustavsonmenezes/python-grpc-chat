import subprocess
import time
import sys
import os

def start_system():
    processes = []
    
    try:
        # Start naming service
        print("🚀 Iniciando Serviço de Nomes...")
        naming_process = subprocess.Popen([sys.executable, 'naming_server.py'])
        processes.append(naming_process)
        time.sleep(2)
        
        # Start primary server
        print("🚀 Iniciando Servidor Primário...")
        primary_process = subprocess.Popen([sys.executable, 'server.py', 'server_1', '50052'])
        processes.append(primary_process)
        time.sleep(3)
        
        # Start backup server
        print("🚀 Iniciando Servidor de Backup...")
        backup_process = subprocess.Popen([sys.executable, 'backup_server.py', 'backup_1', '50053', 'localhost:50052'])
        processes.append(backup_process)
        time.sleep(2)
        
        print("\n✅ Sistema iniciado!")
        print("   - Serviço de Nomes: localhost:50051")
        print("   - Servidor Primário: localhost:50052") 
        print("   - Servidor Backup: localhost:50053")
        print("\n💡 Execute 'python client.py' para conectar")
        print("💡 Use Ctrl+C para parar o sistema")
        
        # Wait for processes
        for process in processes:
            process.wait()
            
    except KeyboardInterrupt:
        print("\n🛑 Parando sistema...")
        for process in processes:
            process.terminate()
        for process in processes:
            process.wait(timeout=5)
        print("✅ Sistema parado")

if __name__ == '__main__':
    start_system()