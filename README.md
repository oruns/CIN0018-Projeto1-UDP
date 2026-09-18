# Projeto 1 - Transmissão de Arquivos com UDP

**Disciplina:** CIN0018 - Fundamentos de redes de computadores  
**Docente:** Renato Mariz de Moraes  
**Grupo:** 2  

## Integrantes
* Anderson Vitor Leoncio de Lima
* [INSERIR NOME DO SEGUNDO INTEGRANTE]
* [INSERIR NOME DO TERCEIRO INTEGRANTE]

## Descrição do Projeto
Aplicação cliente-servidor desenvolvida em Python para a transmissão de ficheiros utilizando o protocolo UDP. O sistema divide ficheiros em pacotes de até 1024 bytes para o envio, armazena-os no servidor (com o prefixo `cliente_`) e permite ao cliente recuperar o ficheiro original de forma a atestar o sucesso da transmissão.

## Pré-requisitos
* Python 3 instalado no sistema.
* Uma interface de linha de comandos (Terminal/Prompt).

## Instruções de Execução

O sistema deve ser testado utilizando dois terminais simultâneos (um para o servidor e outro para o cliente).

### 1. Iniciar o Servidor
No primeiro terminal, navegue até a pasta do projeto e inicie o servidor:
```bash
python3 server.py

# CIN0018-Projeto1-UDP
