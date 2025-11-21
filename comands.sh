# Limpiar todo
pkill -f chord-node || echo "No processes to kill"

ss -tlnp | grep -E ':(800[0-9])' || echo "All chord ports are free"

# Iniciar el bootstrap

cd /home/i298832/chord-dht && ./bin/chord-node --addr 0.0.0.0:8000 --public 34.38.96.126:8000 --metrics results/vm1.csv --id bootstrap-vm1

grpcurl -plaintext localhost:8000 proto.ChordService/GetInfo | head -10

# Iniciar el nodo 2
cd /home/i298832/CHORD-DHT
./bin/chord-node --addr 0.0.0.0:8001 --public 35.199.69.216:8001 --bootstrap 34.38.96.126:8000 --metrics results/vm2.csv --id join-vm2
grpcurl -plaintext localhost:8001 proto.grpcurl -plaintext localhost:8001 proto.ChordService/GetInfo | grep -E "node|predecessor|successor" -A2grpcurl -plaintext localhost:8001 proto.ChordService/GetInfo | grep -E "node|predecessor|successor" -A2grpcurl -plaintext localhost:8001 proto.ChordService/GetInfo | grep -E "node|predecessor|successor" -A2grpcurl -plaintext localhost:8001 proto.ChordService/GetInfo | grep -E "node|predecessor|successor" -A2ChordService/GetInfo | grep -E "node|predecessor|successor" -A2

#Iniciar el nodo 3
cd /home/i298832/CHORD-DHT
./bin/chord-node --addr 0.0.0.0:8002 --public 34.58.253.117:8002 --bootstrap 34.38.96.126:8000 --metrics results/vm3.csv --id join-vm31
grpcurl -plaintext localhost:8002 proto.ChordService/GetInfo | grep -E "node|predecessor|successor" -A2

# kill all nodes
pkill -f chord-node

#compile
cd /home/i298832/chord-dht && make build

# push and pull
git add . && git commit -m "Update" && git push origin main
git pull origin main