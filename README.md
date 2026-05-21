# Aether: High-Performance Distributed Systems Core

O Aether é um runtime de orquestração e computação distribuída de baixa latência escrito em Rust. O sistema utiliza Protocol Buffers (Protobuf) sobre transporte gRPC (HTTP/2 Multiplexed) para estabelecer um protocolo de comunicação fortemente tipado, assíncrono e de alto alinhamento de memória entre os nós do cluster (Master/Workers).

Projetado para ambientes de infraestrutura crítica, o Aether elimina o overhead de Garbage Collection e garante determinismo na gerência de recursos através do modelo de ownership do Rust, operando com consumo de memória previsível e saturação de I/O eficiente.

---

## System Architecture & Codegen Pipeline

A integridade das interfaces de rede do cluster é garantida em tempo de compilação. O script de automação do Cargo (build.rs) intercepta o pipeline de build para compilar as definições agnósticas de IDL (.proto) usando geradores de código altamente otimizados (prost / tonic-build).


graph TD
    subgraph "Compile-Time (Source & Codegen)"
        IDL[proto/*.proto] -->|Define IPC Contracts| BR[build.rs]
        BR -->|Prost / Tonic Compiler| Native[Generated Rust Types & gRPC Stubs]
    end

    subgraph "Runtime (Aether Nodes Topology)"
        Master[Aether Master Node] <-->|gRPC Bidirectional Streams / HTTP/2| Worker1[Worker Node 01]
        Master <-->|gRPC Bidirectional Streams / HTTP/2| Worker2[Worker Node 02]
    end

    Native -.->|Injected into| Master
    Native -.->|Injected into| Worker1
    Native -.->|Injected into| Worker2

Architectural Decisions & Technical Trade-offs

    Zero-Copy Serialization: Utilização de buffers eficientes para minimizar a alocação dinâmica e cópia de memória durante a serialização/desserialização de payloads grandes na rede.

    Asynchronous I/O Multiplexing: Construído sobre o ecossistema assíncrono tokio, utilizando epoll nativo do Linux no backend para gerenciar milhares de conexões simultâneas e concorrência orientada a eventos sem travamento de threads (Non-blocking I/O).

    HTTP/2 Bidirectional Streaming: Permite que o Master e os Workers enviem mensagens de controle e telemetria de forma concorrente sobre a mesma conexão TCP fixa, reduzindo drasticamente o overhead de handshake.

Technology Stack & Requirements

    Core Language: Rust Core v1.95.0+ (Stable Toolchain)

    Transport & RPC Layer: gRPC / HTTP/2

    Data Serialization: Protocol Buffers v3

    Runtime Assíncrono: Tokio (Multi-threaded scheduler)

Repository Topology

    proto/ - Definições de IDL contendo as estruturas das mensagens de rede (Payloads) e assinaturas dos serviços do cluster.

    src/ - Implementação do motor distribuído (gerenciamento de estado, nós de processamento e lógica de rede).

    build.rs - Script de meta-programação responsável por garantir a compilação determinística do Protobuf antes do build do binário principal.

    Cargo.toml - Manifesto de dependências do ecossistema Rust e configurações finas de perfis de otimização (release).

Bootstrap & Compilação
1. Dependências do Sistema (Arch Linux)

O compilador de protocolo (protoc) é obrigatório para traduzir os arquivos IDL do gRPC:
Bash

sudo pacman -S protobuf rustup

2. Pipeline de Build Otimizado

Para compilar o binário com todas as otimizações de loop, inline de funções e remoção de símbolos de debug ativos:
Bash

cargo build --release

Roadmap de Engenharia (Observabilidade & Baixo Nível)

    [ ] Implementar Telemetria Distribuída e métricas expostas via Prometheus.

    [ ] Integração de Rede Avançada: Acoplar o processamento do cluster com filtros de pacotes de baixo nível via eBPF/XDP (módulo externo), mitigando ataques de rede direto na camada de driver antes de subir para o Userspace.
    EOF
