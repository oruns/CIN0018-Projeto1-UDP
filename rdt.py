import socket
import random

# Configurações do RDT
LOSS_PROB = 0.15      # 15% de probabilidade de perda de pacote
CORRUPT_PROB = 0.15   # 15% de probabilidade de corrupção do pacote
TIMEOUT = 0.05         # 50ms de timeout

class RDTSocket:
    def __init__(self, sock):
        self.sock = sock
        self.sock.settimeout(TIMEOUT)
        self.seq_send = 0
        self.seq_recv = 0
        self.recv_buffer = []
        self.stats = {'timeouts': 0, 'naks_recv': 0, 'naks_sent': 0}
        
    def print_and_clear_stats(self):
        t = self.stats['timeouts']
        nr = self.stats['naks_recv']
        ns = self.stats['naks_sent']
        print(f"[RDT 3.0] Resumo -> Timeouts: {t} | NAKs enviados: {ns} | NAKs recebidos: {nr}")
        self.stats = {'timeouts': 0, 'naks_recv': 0, 'naks_sent': 0}


        
    def send_pkt(self, pkt, addr):
        """Envia o pacote pela rede, simulando perdas aleatórias."""
        if random.random() > LOSS_PROB:
            self.sock.sendto(pkt, addr)
        else:
            pass

    def rdt_send(self, data, addr):
        """ Envia dados de forma confiável usando RDT 3.0 (Stop-and-Wait) """
        pkt = b'D' + bytes([self.seq_send]) + data
        retries = 0
        
        while retries < 20:
            self.send_pkt(pkt, addr)
            
            try:
                # Aguarda ACK ou NAK
                recv_pkt, recv_addr = self.sock.recvfrom(65535)
                
                # Simula perda no recebimento do ACK/NAK
                if random.random() <= LOSS_PROB:
                    pass
                    continue
                    
                if len(recv_pkt) < 2:
                    continue
                    
                pkt_type = recv_pkt[0:1]
                ack_seq = recv_pkt[1]
                
                if pkt_type == b'A':
                    if ack_seq == self.seq_send:
                        # ACK correto recebido
                        self.seq_send = 1 - self.seq_send
                        break
                    else:
                        pass
                elif pkt_type == b'N':
                    if ack_seq == self.seq_send:
                        self.stats['naks_recv'] += 1
                elif pkt_type == b'D':
                    # Recebeu um pacote de dados enquanto esperava ACK.
                    if ack_seq == self.seq_recv:
                        # Pacote novo. Não envia ACK ainda, deixa para o rdt_recv processar.
                        self.recv_buffer.append((recv_pkt, recv_addr))
                    else:
                        # Pacote antigo. Envia ACK cruzado.
                        self.send_pkt(b'A' + bytes([ack_seq]), recv_addr)
                        
            except socket.timeout:
                retries += 1
                self.stats['timeouts'] += 1
        
        if retries >= 20:
            pass
            self.seq_send = 1 - self.seq_send

    def rdt_recv(self):
        """ Recebe dados de forma confiável usando RDT 3.0 """
        while True:
            if self.recv_buffer:
                pkt, addr = self.recv_buffer.pop(0)
            else:
                try:
                    pkt, addr = self.sock.recvfrom(65535)
                    # Simula perda no recebimento
                    if random.random() <= LOSS_PROB:
                        pass
                        continue
                except socket.timeout:
                    continue
            
            if len(pkt) < 2:
                continue
                
            pkt_type = pkt[0:1]
            seq = pkt[1]
            data = pkt[2:]
            
            if pkt_type == b'D':
                # Simula corrupção de dados
                if random.random() <= CORRUPT_PROB:
                    self.stats['naks_sent'] += 1
                    self.send_pkt(b'N' + bytes([seq]), addr)
                    continue

                if seq == self.seq_recv:
                    # Pacote esperado
                    self.send_pkt(b'A' + bytes([seq]), addr)
                    self.seq_recv = 1 - self.seq_recv
                    return data, addr
                else:
                    # Pacote duplicado / fora de ordem
                    pass
                    self.send_pkt(b'A' + bytes([seq]), addr)
            elif pkt_type == b'A' or pkt_type == b'N':
                # ACKs/NAKs atrasados ignorados na recepção de dados
                pass
