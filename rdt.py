"""
Canal de transferência confiável RDT 3.0 (Stop-and-Wait) sobre sockets UDP.

Cabeçalho de 4 bytes: [TIPO (1)] [SEQ (1)] [CHECKSUM (2)] [DADOS]

O transmissor percorre os quatro estados da FSM do rdt3.0 (Espera chamada 0 de
cima, Espera por ACK0, Espera chamada 1 de cima, Espera por ACK1) e o receptor
alterna entre Esperar 0 de baixo e Esperar 1 de baixo.

O receptor confirma sempre o último pacote que aceitou, então um ACK repetido
é o que avisa o transmissor de que o pacote atual precisa ser reenviado. A perda
total do pacote fica por conta do temporizador.

A simulação do canal está em send_pkt: um pacote entregue a ela pode sumir ou
ter um bit trocado antes de chegar ao destino.
"""

import socket
import random

# Configurações do RDT
LOSS_PROB = 0.15      # 15% de probabilidade de perda no canal
CORRUPT_PROB = 0.15   # 15% de probabilidade de corrupção no canal
TIMEOUT = 0.05        # 50ms de timeout
VERBOSE = True        # Registra cada evento do protocolo em tempo de execução

HEADER_SIZE = 4

# Estados das FSMs do rdt3.0, indexados pelo número de sequência.
ESPERA_CHAMADA = ("Espera chamada 0 de cima", "Espera chamada 1 de cima")
ESPERA_ACK = ("Espera por ACK0", "Espera por ACK1")
ESPERAR_DE_BAIXO = ("Esperar 0 de baixo", "Esperar 1 de baixo")


def checksum(data):
    """Soma de verificação de 16 bits em complemento de um."""
    total = 0
    for i in range(0, len(data) - 1, 2):
        total += (data[i] << 8) + data[i + 1]
    if len(data) % 2:
        total += data[-1] << 8
    while total >> 16:
        total = (total & 0xFFFF) + (total >> 16)
    return ~total & 0xFFFF


def make_pkt(pkt_type, seq, data=b""):
    """Monta o pacote com o checksum de tipo, seq e dados."""
    protegido = pkt_type + bytes([seq]) + data
    return pkt_type + bytes([seq]) + checksum(protegido).to_bytes(2, "big") + data


def parse_pkt(pkt):
    """Desmonta o pacote e confere o checksum.

    Devolve (tipo, seq, dados, integro); tipo é None se vier menor que o cabeçalho.
    """
    if len(pkt) < HEADER_SIZE:
        return None, None, b"", False

    pkt_type = pkt[0:1]
    seq = pkt[1]
    recebido = int.from_bytes(pkt[2:4], "big")
    data = pkt[HEADER_SIZE:]

    return pkt_type, seq, data, checksum(pkt_type + bytes([seq]) + data) == recebido


class RDTSocket:
    def __init__(self, sock):
        self.sock = sock
        self.sock.settimeout(TIMEOUT)
        self.seq_send = 0
        self.seq_recv = 0
        self.recv_buffer = []
        self.stats = self._zerar_stats()

    @staticmethod
    def _zerar_stats():
        return {
            "timeouts": 0,
            "acks_repetidos": 0,
            "acks_duplicados": 0,
            "retransmissoes": 0,
            "perdas": 0,
            "corrupcoes": 0,
            "corrompidos_detectados": 0,
        }

    def log(self, estado, transicao):
        """Registra uma transição da FSM: o estado de origem e o que ela faz."""
        if VERBOSE:
            print(f"[RDT]       [{estado}] {transicao}", flush=True)

    def log_canal(self, mensagem):
        if VERBOSE:
            print(f"[RDT]       (canal) {mensagem}", flush=True)

    def print_and_clear_stats(self):
        s = self.stats
        print(
            f"[RDT 3.0] Resumo -> Timeouts: {s['timeouts']} | "
            f"Retransmissões: {s['retransmissoes']} | "
            f"ACKs reenviados: {s['acks_repetidos']} | "
            f"ACKs duplicados recebidos: {s['acks_duplicados']} | "
            f"Perdas no canal: {s['perdas']} | Corrupções no canal: {s['corrupcoes']} | "
            f"Corrompidos detectados: {s['corrompidos_detectados']}"
        )
        self.stats = self._zerar_stats()

    def send_pkt(self, pkt, addr):
        """Entrega o pacote ao canal, que pode perdê-lo ou corrompê-lo.

        O mesmo sorteio decide as duas coisas, então elas são exclusivas e cada
        uma mantém a probabilidade configurada.
        """
        sorteio = random.random()

        if sorteio < LOSS_PROB:
            self.stats["perdas"] += 1
            self.log_canal(f"PERDEU {self._descrever(pkt)}")
            return

        if sorteio < LOSS_PROB + CORRUPT_PROB:
            self.stats["corrupcoes"] += 1
            self.log_canal(f"CORROMPEU {self._descrever(pkt)}")
            pkt = self._corromper(pkt)

        self.sock.sendto(pkt, addr)

    @staticmethod
    def _descrever(pkt):
        rotulo = {b"D": "dados", b"A": "ACK"}.get(pkt[0:1], "pacote")
        return f"{rotulo} seq={pkt[1]}"

    @staticmethod
    def _corromper(pkt):
        """Inverte um bit, preservando só o byte de tipo."""
        corrompido = bytearray(pkt)
        posicao = random.randrange(1, len(corrompido))
        corrompido[posicao] ^= 1 << random.randrange(8)
        return bytes(corrompido)

    def rdt_send(self, data, addr):
        """FSM do transmissor: Espera chamada N de cima -> Espera por ACKN.

        Só volta quando o ACK correspondente chega, então a entrega não depende
        da taxa de perda do canal.
        """
        seq = self.seq_send
        sndpkt = make_pkt(b"D", seq, data)

        self.log(ESPERA_CHAMADA[seq], "rdt_send(data) -> udt_send(sndpkt), start_timer")
        self.send_pkt(sndpkt, addr)

        # Daqui em diante o estado é ESPERA_ACK[seq].
        while True:
            try:
                rcvpkt, rcv_addr = self.sock.recvfrom(65535)
            except socket.timeout:
                self.stats["timeouts"] += 1
                self.stats["retransmissoes"] += 1
                self.log(ESPERA_ACK[seq], "timeout -> udt_send(sndpkt), start_timer")
                self.send_pkt(sndpkt, addr)
                continue

            pkt_type, rcv_seq, _, integro = parse_pkt(rcvpkt)

            if not integro:
                # corrupt(rcvpkt): nada a fazer, o temporizador resolve.
                self.stats["corrompidos_detectados"] += 1
                self.log(ESPERA_ACK[seq], "rdt_rcv && corrupt(rcvpkt) -> Λ")
                continue

            if pkt_type == b"A":
                if rcv_seq == seq:
                    self.seq_send = 1 - seq
                    self.log(
                        ESPERA_ACK[seq],
                        f"rdt_rcv && notcorrupt && isACK({seq}) -> stop_timer "
                        f"=> {ESPERA_CHAMADA[self.seq_send]}",
                    )
                    return

                # isACK do pacote anterior: o receptor está pedindo este de novo.
                self.stats["acks_duplicados"] += 1
                self.stats["retransmissoes"] += 1
                self.log(
                    ESPERA_ACK[seq],
                    f"rdt_rcv && isACK({rcv_seq}) -> udt_send(sndpkt), start_timer",
                )
                self.send_pkt(sndpkt, addr)

            elif pkt_type == b"D":
                # Dados chegando enquanto esperamos o ACK, porque os dois
                # sentidos dividem o mesmo socket.
                if rcv_seq == self.seq_recv:
                    self.recv_buffer.append((rcvpkt, rcv_addr))
                else:
                    self.send_pkt(make_pkt(b"A", rcv_seq), rcv_addr)

    def rdt_recv(self):
        """FSM do receptor: alterna entre Esperar 0 de baixo e Esperar 1 de baixo."""
        while True:
            esperado = self.seq_recv
            estado = ESPERAR_DE_BAIXO[esperado]

            if self.recv_buffer:
                rcvpkt, addr = self.recv_buffer.pop(0)
            else:
                try:
                    rcvpkt, addr = self.sock.recvfrom(65535)
                except socket.timeout:
                    continue

            pkt_type, seq, data, integro = parse_pkt(rcvpkt)

            if pkt_type != b"D":
                continue

            if not integro:
                # O seq veio dentro da área protegida, então não serve de
                # referência: a resposta sai do estado local.
                ultimo_aceito = 1 - esperado
                self.stats["corrompidos_detectados"] += 1
                self.stats["acks_repetidos"] += 1
                self.log(
                    estado,
                    f"rdt_rcv && corrupt(rcvpkt) -> udt_send(ACK{ultimo_aceito})",
                )
                self.send_pkt(make_pkt(b"A", ultimo_aceito), addr)
                continue

            if seq == esperado:
                self.seq_recv = 1 - esperado
                self.log(
                    estado,
                    f"rdt_rcv && notcorrupt && has_seq{esperado} -> extract, "
                    f"deliver_data, udt_send(ACK{esperado}) "
                    f"=> {ESPERAR_DE_BAIXO[self.seq_recv]}",
                )
                self.send_pkt(make_pkt(b"A", esperado), addr)
                return data, addr

            # Duplicata: confirma de novo o pacote que já tinha sido aceito.
            self.stats["acks_repetidos"] += 1
            self.log(estado, f"rdt_rcv && has_seq{seq} -> udt_send(ACK{seq})")
            self.send_pkt(make_pkt(b"A", seq), addr)
