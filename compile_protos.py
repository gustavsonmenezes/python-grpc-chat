import subprocess
import os
import sys

def compile_protos():
    """Compile all .proto files"""
    
    # Find all .proto files
    proto_files = [f for f in os.listdir('.') if f.endswith('.proto')]
    
    if not proto_files:
        print("No .proto files found!")
        return False
    
    for proto_file in proto_files:
        print(f"Compiling {proto_file}...")
        
        try:
            # Generate _pb2.py and _pb2_grpc.py
            subprocess.run([
                'python', '-m', 'grpc_tools.protoc',
                f'--proto_path=.',
                f'--python_out=.',
                f'--grpc_python_out=.',
                proto_file
            ], check=True)
            
            print(f"✅ {proto_file} compiled successfully!")
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Error compiling {proto_file}: {e}")
            return False
    
    return True

if __name__ == '__main__':
    if compile_protos():
        print("\n🎉 All proto files compiled successfully!")
        print("Generated files:")
        for file in os.listdir('.'):
            if file.endswith('_pb2.py') or file.endswith('_pb2_grpc.py'):
                print(f"  - {file}")
    else:
        print("\n💥 Failed to compile proto files")
        sys.exit(1)