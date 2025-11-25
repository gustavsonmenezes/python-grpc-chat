import grpc
from concurrent import futures
import threading
import time
import logging
from datetime import datetime, timedelta

import naming_service_pb2
import naming_service_pb2_grpc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NamingServiceServicer(naming_service_pb2_grpc.NamingServiceServicer):
    def __init__(self):
        self.servers = {}  # {server_id: {address, last_heartbeat, is_primary, status}}
        self.lock = threading.Lock()
        self.cleanup_interval = 30  # seconds
        
        # Start cleanup thread
        self.cleanup_thread = threading.Thread(target=self._cleanup_expired_servers, daemon=True)
        self.cleanup_thread.start()
    
    def RegisterServer(self, request, context):
        server_id = request.server_id
        address = request.address
        is_primary = request.is_primary
        
        with self.lock:
            self.servers[server_id] = {
                'address': address,
                'last_heartbeat': datetime.now(),
                'is_primary': is_primary,
                'status': 'active'
            }
        
        logger.info(f"Servidor registrado: {server_id} ({address}) - Primário: {is_primary}")
        return naming_service_pb2.RegisterResponse(success=True)
    
    def Heartbeat(self, request, context):
        server_id = request.server_id
        
        with self.lock:
            if server_id in self.servers:
                self.servers[server_id]['last_heartbeat'] = datetime.now()
                self.servers[server_id]['status'] = 'active'
                return naming_service_pb2.HeartbeatResponse(success=True)
            else:
                return naming_service_pb2.HeartbeatResponse(success=False)
    
    def LookupServer(self, request, context):
        with self.lock:
            # Find primary server
            for server_id, server_info in self.servers.items():
                if server_info['is_primary'] and server_info['status'] == 'active':
                    return naming_service_pb2.LookupResponse(
                        server_id=server_id,
                        address=server_info['address'],
                        success=True
                    )
            
            # If no primary found, try to find any active server
            for server_id, server_info in self.servers.items():
                if server_info['status'] == 'active':
                    # Promote to primary
                    server_info['is_primary'] = True
                    logger.info(f"Servidor {server_id} promovido a primário")
                    return naming_service_pb2.LookupResponse(
                        server_id=server_id,
                        address=server_info['address'],
                        success=True
                    )
            
            return naming_service_pb2.LookupResponse(success=False)
    
    def ListServers(self, request, context):
        with self.lock:
            servers_list = []
            for server_id, server_info in self.servers.items():
                servers_list.append(naming_service_pb2.ServerInfo(
                    server_id=server_id,
                    address=server_info['address'],
                    is_primary=server_info['is_primary'],
                    status=server_info['status'],
                    last_heartbeat=server_info['last_heartbeat'].isoformat()
                ))
            return naming_service_pb2.ListServersResponse(servers=servers_list)
    
    def _cleanup_expired_servers(self):
        """Remove servers that haven't sent heartbeat for a while"""
        while True:
            time.sleep(self.cleanup_interval)
            with self.lock:
                now = datetime.now()
                expired_servers = []
                
                for server_id, server_info in self.servers.items():
                    if now - server_info['last_heartbeat'] > timedelta(seconds=60):  # 60 seconds timeout
                        expired_servers.append(server_id)
                
                for server_id in expired_servers:
                    logger.warning(f"Removendo servidor expirado: {server_id}")
                    del self.servers[server_id]

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    naming_service_pb2_grpc.add_NamingServiceServicer_to_server(
        NamingServiceServicer(), server
    )
    server.add_insecure_port('[::]:50051')
    server.start()
    logger.info("Serviço de Nomes iniciado na porta 50051")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Desligando Serviço de Nomes...")

if __name__ == '__main__':
    serve()