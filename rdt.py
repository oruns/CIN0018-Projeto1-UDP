"""
Canal de transferência confiável RDT 3.0 (Stop-and-Wait) sobre sockets UDP.

Cabeçalho de 3 bytes: [SEQ (1)] [CHECKSUM (2)] [DADOS]

O transmissor e o receptor são máquinas separadas, e cada uma tem o seu canal,
como no modelo. O canal de dados leva os pacotes do transmissor de um lado até
o receptor do outro, e o canal de confirmação leva os ACKs de volta. Nenhum
pacote precisa dizer o que é, porque o canal por onde ele chegou já determina
a qual máquina pertence.

Cada lado abre um socket por canal e usa select para esperar nos dois ao mesmo
tempo. É o que permite ao lado que está transmitindo continuar atendendo os
pacotes que chegam para o seu receptor.

A simulação do canal está em send_pkt, um pacote entregue a ela pode sumir ou
ter um bit trocado antes de chegar ao destino.
"""

import random
import select
import time

# Configurações do RDT
LOSS_PROB = 0.15      # 15% de probabilidade de perda no canal
CORRUPT_PROB = 0.15   # 15% de probabilidade de corrupção no canal
TIMEOUT = 0.05        # 50ms de timeout
VERBOSE = True        # Registra cada evento do protocolo em tempo de execução

ACK = b"A"            # identificador que o livro escreve em make_pkt(ACK, seq, checksum)

HEADER_SIZE = 3
ACK_SIZE = 4
PAYLOAD_MAX = 2**10   # maior payload que a camada de cima entrega de uma vez
RECV_SIZE = HEADER_SIZE + PAYLOAD_MAX
ESPERA_CURTA = 0.05   # quanto esperar nos canais quando nenhum prazo corre


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


def make_pkt(seq, data=b""):
    """Monta o pacote de dados, o make_pkt(seq, data, checksum) do livro."""
    return bytes([seq]) + checksum(bytes([seq]) + data).to_bytes(2, "big") + data


def make_ack(seq):
    """Monta o pacote de confirmação, o make_pkt(ACK, seq, checksum) do livro."""
    protegido = ACK + bytes([seq])
    return protegido + checksum(protegido).to_bytes(2, "big")


def parse_ack(pkt):
    """Desmonta uma confirmação e confere o identificador e o checksum.

    Devolve (seq, integro). Um pacote que não traga o identificador ACK conta
    como corrompido, porque é o que ele seria num canal que troca bits.
    """
    if len(pkt) != ACK_SIZE or pkt[0:1] != ACK:
        return None, False

    seq = pkt[1]
    return seq, checksum(ACK + bytes([seq])) == int.from_bytes(pkt[2:4], "big")


def parse_pkt(pkt):
    """Desmonta o pacote e confere o checksum.

    Devolve (seq, dados, integro), com seq None se vier menor que o cabeçalho.
    """
    if len(pkt) < HEADER_SIZE:
        return None, b"", False

    seq = pkt[0]
    recebido = int.from_bytes(pkt[1:3], "big")
    data = pkt[HEADER_SIZE:]

    return seq, data, checksum(bytes([seq]) + data) == recebido


class RDTSocket:
    def __init__(self, sock_receptor, sock_transmissor):
        self.sock_receptor = sock_receptor        # escuta o canal de dados, responde os ACKs
        self.sock_transmissor = sock_transmissor  # emite no canal de dados, escuta os ACKs
        self.seq_send = 0              # estado da máquina do transmissor
        self.seq_recv = 0              # estado da máquina do receptor
        self.entregues = []
        self.stats = self._zerar_stats()

    @staticmethod
    def _zerar_stats():
        return {
            "timeouts": 0,
            "acks_repetidos": 0,
            "acks_ignorados": 0,
            "retransmissoes": 0,
            "perdas": 0,
            "corrupcoes": 0,
            "corrompidos_detectados": 0,
        }

    def log(self, mensagem):
        if VERBOSE:
            print(f"[RDT]       {mensagem}", flush=True)

    def log_canal(self, mensagem):
        if VERBOSE:
            print(f"[RDT]       (canal) {mensagem}", flush=True)

    def print_and_clear_stats(self):
        s = self.stats
        print(
            f"[RDT 3.0] Resumo -> Timeouts: {s['timeouts']} | "
            f"Retransmissões: {s['retransmissoes']} | "
            f"ACKs reenviados: {s['acks_repetidos']} | "
            f"ACKs ignorados: {s['acks_ignorados']} | "
            f"Perdas no canal: {s['perdas']} | Corrupções no canal: {s['corrupcoes']} | "
            f"Corrompidos detectados: {s['corrompidos_detectados']}"
        )
        self.stats = self._zerar_stats()

    def send_pkt(self, pkt, sock, addr, rotulo, seq):
        """Entrega o pacote ao canal, que pode perdê-lo ou corrompê-lo.

        O mesmo sorteio decide as duas coisas, então elas são exclusivas e cada
        uma mantém a probabilidade configurada.
        """
        sorteio = random.random()

        if sorteio < LOSS_PROB:
            self.stats["perdas"] += 1
            self.log_canal(f"PERDEU {rotulo} seq={seq}")
            return

        if sorteio < LOSS_PROB + CORRUPT_PROB:
            self.stats["corrupcoes"] += 1
            self.log_canal(f"CORROMPEU {rotulo} seq={seq}")
            pkt = self._corromper(pkt)

        sock.sendto(pkt, addr)

    @staticmethod
    def _corromper(pkt):
        """Inverte um bit qualquer do pacote, cabeçalho incluído."""
        corrompido = bytearray(pkt)
        posicao = random.randrange(len(corrompido))
        corrompido[posicao] ^= 1 << random.randrange(8)
        return bytes(corrompido)

    def _espera(self, prazo):
        """Espera chegar algo em qualquer um dos dois canais.

        Devolve (seq, integro) quando veio um ACK pelo canal de confirmação, que
        é o que interessa ao transmissor. Um pacote que chegue pelo canal de
        dados é tratado na hora pelo receptor, e aí o retorno é None, assim como
        quando nada chega. O prazo é o instante em que o temporizador dispara,
        ou None quando nenhum envio está em curso.
        """
        restante = (prazo - time.monotonic()) if prazo else ESPERA_CURTA
        if restante <= 0:
            return None

        pronto, _, _ = select.select([self.sock_receptor, self.sock_transmissor], [], [], restante)

        ack = None
        for canal in pronto:
            if canal is self.sock_transmissor:
                rcvpkt, _ = canal.recvfrom(RECV_SIZE)
                ack = parse_ack(rcvpkt)
            else:
                rcvpkt, addr = canal.recvfrom(RECV_SIZE)
                seq, data, integro = parse_pkt(rcvpkt)
                self._receptor(seq, data, integro, addr)

        return ack

    def _receptor(self, seq, data, integro, addr):
        """Máquina do receptor, acionada a cada pacote do canal de dados."""
        esperado = self.seq_recv

        # ------------------------------------------------------------------
        # Estado "Esperar N de baixo"
        #
        #   rdt_rcv(rcvpkt) && (corrupt(rcvpkt) || has_seq(1-N)(rcvpkt))
        #     sndpkt = make_pkt(ACK, 1-N, checksum)
        #     udt_send(sndpkt)
        # ------------------------------------------------------------------
        if not integro or seq != esperado:
            anterior = 1 - esperado
            if not integro:
                self.stats["corrompidos_detectados"] += 1
                self.log(f"checksum inválido, reenvia ACK {anterior}")
            else:
                self.log(f"pacote {seq} duplicado, reenvia ACK {anterior}")
            self.stats["acks_repetidos"] += 1
            self.send_pkt(make_ack(anterior), self.sock_receptor, addr, "ACK", anterior)
            return

        # ------------------------------------------------------------------
        #   rdt_rcv(rcvpkt) && notcorrupt(rcvpkt) && has_seqN(rcvpkt)
        #     extract(rcvpkt, data)
        #     deliver_data(data)
        #     sndpkt = make_pkt(ACK, N, checksum)
        #     udt_send(sndpkt)
        #
        # Vai para "Esperar 1-N de baixo"
        # ------------------------------------------------------------------
        self.seq_recv = 1 - esperado
        self.log(f"pacote {esperado} íntegro, envia ACK {esperado}")
        self.entregues.append((data, addr))
        self.send_pkt(make_ack(esperado), self.sock_receptor, addr, "ACK", esperado)

    def rdt_send(self, data, addr):
        """Máquina do transmissor."""
        seq = self.seq_send
        sndpkt = make_pkt(seq, data)

        # ------------------------------------------------------------------
        # Estado "Esperar chamada N de cima"
        #
        #   rdt_send(data)
        #     sndpkt = make_pkt(N, data, checksum)
        #     udt_send(sndpkt)
        #     start_timer
        # ------------------------------------------------------------------
        self.log(f"envia pacote {seq}")
        self.send_pkt(sndpkt, self.sock_transmissor, addr, "dados", seq)
        prazo = time.monotonic() + TIMEOUT

        # ------------------------------------------------------------------
        # Estado "Esperar ACK N"
        #
        # O temporizador é armado no envio e rearmado só quando dispara, então
        # nada que chegue no meio do caminho estica o prazo
        # ------------------------------------------------------------------
        while True:
            ack = self._espera(prazo)

            if ack is None:
                if time.monotonic() >= prazo:
                    # ----------------------------------------------------
                    #   timeout
                    #     udt_send(sndpkt)
                    #     start_timer
                    # ----------------------------------------------------
                    self.stats["timeouts"] += 1
                    self.stats["retransmissoes"] += 1
                    self.log("timeout, retransmite")
                    self.send_pkt(sndpkt, self.sock_transmissor, addr, "dados", seq)
                    prazo = time.monotonic() + TIMEOUT
                continue

            ack_seq, integro = ack

            if not integro or ack_seq != seq:
                # --------------------------------------------------------
                #   rdt_rcv(rcvpkt) && (corrupt(rcvpkt) || isACK(rcvpkt, 1-N))
                #     Λ
                # --------------------------------------------------------
                if not integro:
                    self.stats["corrompidos_detectados"] += 1
                    self.log("ACK corrompido, ignora")
                else:
                    self.stats["acks_ignorados"] += 1
                    self.log(f"ACK {ack_seq} fora de ordem, ignora")
                continue

            # ------------------------------------------------------------
            #   rdt_rcv(rcvpkt) && notcorrupt(rcvpkt) && isACK(rcvpkt, N)
            #     stop_timer
            #
            # Vai para "Esperar chamada 1-N de cima"
            # ------------------------------------------------------------
            self.seq_send = 1 - seq
            self.log(f"ACK {seq} recebido")
            return

    def rdt_recv(self):
        """Entrega à camada de cima o próximo dado que o receptor aceitou."""
        while not self.entregues:
            # ------------------------------------------------------------
            # Estado "Esperar chamada N de cima", do lado do transmissor
            #
            #   rdt_rcv(rcvpkt)
            #     Λ
            #
            # Nenhum envio está em curso, então um ACK atrasado que chegue
            # pelo canal de confirmação é descartado
            # ------------------------------------------------------------
            if self._espera(None) is not None:
                self.stats["acks_ignorados"] += 1
                self.log("ACK atrasado fora de envio, ignora")

        return self.entregues.pop(0)
