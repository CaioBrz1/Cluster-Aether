use clap::{Parser, ValueEnum};
use std::collections::{HashMap, VecDeque};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use sysinfo::{Components, CpuRefreshKind, RefreshKind, System};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};

// Importa o código gerado automaticamente pelo arquivo .proto
pub mod aether_grpc {
    tonic::include_proto!("aether");
}

use aether_grpc::master_service_client::MasterServiceClient;
use aether_grpc::master_service_server::{MasterService, MasterServiceServer};
use aether_grpc::worker_service_client::WorkerServiceClient;
use aether_grpc::worker_service_server::{WorkerService, WorkerServiceServer};
use aether_grpc::{HeartbeatRequest, HeartbeatResponse, TaskRequest, TaskResponse};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Mode {
    Master,
    Worker,
    Client,
}

#[derive(Parser, Debug)]
#[command(name = "aether", author = "Caio <CaioBrz1>", version = "0.1.0")]
struct Args {
    #[arg(short, long, value_enum)]
    mode: Mode,

    #[arg(short, long, default_value_t = 50051)]
    port: u16,

    #[arg(short = 'i', long, default_value = "127.0.0.1")]
    master_ip: String,

    #[arg(short = 'c', long, default_value = "")]
    command: String,
}

// -----------------------------------------------------------------
// ENGENHARIA DO MASTER (O CÉREBRO INTERATIVO)
// -----------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct WorkerState {
    pub ip_address: String,
    pub port: u32,
    pub last_seen: Instant,
    pub cpu_usage: f32,
    pub ram_usage: f32,
    pub temperature: f32,
}

impl WorkerState {
    pub fn calcular_capacidade(&self) -> f32 {
        let peso_cpu = (100.0 - self.cpu_usage).max(0.0);
        let peso_termico = if self.temperature > 80.0 {
            (100.0 - (self.temperature - 80.0) * 5.0).max(0.0)
        } else {
            100.0
        };
        (peso_cpu * 0.7) + (peso_termico * 0.3)
    }
}

// ALTA ENGENHARIA: Suporte nativo a fatiamento de micro-lotes
#[derive(Debug, Clone)]
pub struct TarefaPendente {
    pub id: String,
    pub comando_base: String,
    pub sub_lotes: Vec<String>,
    pub lotes_concluidos: usize,
} 

#[derive(Debug, Clone, Default)]
pub struct AetherMaster {
    pub workers: Arc<Mutex<HashMap<String, WorkerState>>>,
    pub fila_tarefas: Arc<Mutex<VecDeque<TarefaPendente>>>,
}

#[tonic::async_trait]
impl MasterService for AetherMaster {
    async fn send_heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let de_onde_veio_cru = request
            .remote_addr()
            .map(|addr| addr.ip().to_string())
            .unwrap_or_else(|| "127.0.0.1".to_string());

        let de_onde_veio = if de_onde_veio_cru == "::1" {
            "127.0.0.1".to_string()
        } else {
            de_onde_veio_cru
        };

        let r = request.into_inner();
        let worker_id = r.worker_id.clone();

        let state = WorkerState {
            ip_address: de_onde_veio,
            port: if r.worker_port == 0 { 50052 } else { r.worker_port },
            last_seen: Instant::now(),
            cpu_usage: r.cpu_usage,
            ram_usage: r.ram_usage,
            temperature: r.temperature,
        };

        let is_new_worker = {
            let mut workers_map = self.workers.lock().unwrap();
            workers_map.insert(worker_id.clone(), state).is_none()
        };

        println!(
            "[MASTER] Telemetria de {} | CPU: {:.1}% | Temp: {:.1}°C",
            worker_id, r.cpu_usage, r.temperature
        );

        if is_new_worker {
            let mut fila = self.fila_tarefas.lock().unwrap();
            
            // Lote simples de teste do sistema
            fila.push_back(TarefaPendente {
                id: "task_01_sysinfo".to_string(),
                comando_base: "uname -a && uptime".to_string(),
                sub_lotes: vec!["uname -a && uptime".to_string()],
                lotes_concluidos: 0,
            });

            // ALTA ENGENHARIA: Quebrando uma tarefa matemática pesada em 3 micro-lotes independentes!
            fila.push_back(TarefaPendente {
                id: "task_02_heavy_math".to_string(),
                comando_base: "pipeline_matematico".to_string(),
                sub_lotes: vec![
                    "echo 'Lote 1/3' && seq 1 2 | while read i; do echo \"Sub-lote A - Item $i\"; sleep 1; done".to_string(),
                    "echo 'Lote 2/3' && seq 3 4 | while read i; do echo \"Sub-lote B - Item $i\"; sleep 1; done".to_string(),
                    "echo 'Lote 3/3' && seq 5 5 | while read i; do echo \"Sub-lote C - Item $i\"; sleep 1; done".to_string(),
                ],
                lotes_concluidos: 0,
            });
            
            println!("[MASTER] Novo Worker registrado! Micro-lotes iniciais injetados na fila.");
        }

        Ok(Response::new(HeartbeatResponse { acknowledged: true }))
    }

    async fn inject_task(
        &self,
        request: Request<TaskRequest>,
    ) -> Result<Response<TaskResponse>, Status> {
        let r = request.into_inner();
        
        let nova_tarefa = TarefaPendente {
            id: format!("client_{}", r.task_id),
            comando_base: r.command.clone(),
            sub_lotes: vec![r.command.clone()], // CLI externa injeta como lote único inicial
            lotes_concluidos: 0,
        };

        {
            let mut fila = self.fila_tarefas.lock().unwrap();
            fila.push_back(nova_tarefa);
        }

        println!("[MASTER] Nova tarefa recebida via CLI externa: '{}'", r.command);

        Ok(Response::new(TaskResponse {
            task_id: r.task_id,
            exit_code: 0,
            stdout: "Tarefa enfileirada no Master com sucesso!\n".to_string(),
            stderr: String::new(),
        }))
    }     
}

// -----------------------------------------------------------------
// ENGENHARIA DO WORKER (OS MÚSCULOS COM STREAMING)
// -----------------------------------------------------------------

#[derive(Debug, Default)]
pub struct AetherWorker {}

#[tonic::async_trait]
impl WorkerService for AetherWorker {
    type ExecuteTaskStream = ReceiverStream<Result<TaskResponse, Status>>;

    async fn execute_task(
        &self,
        request: Request<TaskRequest>,
    ) -> Result<Response<Self::ExecuteTaskStream>, Status> {
        let r = request.into_inner();
        println!("[WORKER] Executando comando remoto em tempo real: {}", r.command);

        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let task_id = r.task_id.clone();

        let mut child = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&r.command)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| Status::internal(format!("Falha ao lançar comando: {}", e)))?;

        tokio::spawn(async move {
            let stdout = child.stdout.take().unwrap();
            let mut reader = BufReader::new(stdout).lines();

            while let Ok(Some(linha)) = reader.next_line().await {
                let response = TaskResponse {
                    task_id: task_id.clone(),
                    exit_code: 0,
                    stdout: format!("{}\n", linha),
                    stderr: String::new(),
                };
                if tx.send(Ok(response)).await.is_err() {
                    break; 
                }
            }

            let status = child.wait().await;
            let exit_code = status.map(|s| s.code().unwrap_or(0)).unwrap_or(-1);

            let _ = tx.send(Ok(TaskResponse {
                task_id,
                exit_code,
                stdout: "[WORKER] --- Micro-lote finalizado ---\n".to_string(),
                stderr: String::new(),
            })).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

// -----------------------------------------------------------------
// FUNÇÃO PRINCIPAL
// -----------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("Iniciando o Aether no modo: {:?}", args.mode);

    match args.mode {
        Mode::Master => {
            let addr = format!("0.0.0.0:{}", args.port).parse()?;
            let master = AetherMaster::default();

            // TASK 1 - REAPER: Remove nós mortos
            let workers_clone = master.workers.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let mut workers = workers_clone.lock().unwrap();
                    let ago = Instant::now();
                    workers.retain(|id, state| {
                        if ago.duration_since(state.last_seen).as_secs() > 6 {
                            println!("[FALHA DETECTADA] Nó '{}' sumiu da rede!", id);
                            false
                        } else {
                            true
                        }
                    });
                }
            });

            // TASK 2 - LOOP DO SCHEDULER DE ALTA VELOCIDADE (DYNAMIC WORK STEALING)
            let workers_sched = master.workers.clone();
            let fila_sched = master.fila_tarefas.clone();

            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(250)).await; // Verificação rápida a cada 250ms
                    
                    let mut lote_para_processar = None;
                    let mut id_tarefa = String::new();

                    // 1. ESCOPO ISOLADO: Extrai apenas UM sub-lote da tarefa atual e libera a fila na hora!
                    {
                        let mut fila = fila_sched.lock().unwrap();
                        if let Some(mut tarefa) = fila.pop_front() {
                            if !tarefa.sub_lotes.is_empty() {
                                id_tarefa = tarefa.id.clone();
                                lote_para_processar = Some(tarefa.sub_lotes.remove(0));
                                tarefa.lotes_concluidos += 1;
                                
                                // Se a tarefa ainda tem mais sub-lotes guardados, ela volta pro fim da fila
                                if !tarefa.sub_lotes.is_empty() {
                                    fila.push_back(tarefa);
                                }
                            }
                        }
                    }

                    if lote_para_processar.is_none() {
                        continue;
                    }
                    let comando_lote = lote_para_processar.unwrap();

                    // 2. ESCOPO ISOLADO: Escolhe por demanda o Worker com maior score livre
                    let mut melhor_worker: Option<(String, String, u32, f32)> = None;
                    {
                        let workers = workers_sched.lock().unwrap();
                        for (id, state) in workers.iter() {
                            let score = state.calcular_capacidade();
                            if score > 30.0 {
                                if melhor_worker.is_none() || score > melhor_worker.as_ref().unwrap().3 {
                                    melhor_worker = Some((id.clone(), state.ip_address.clone(), state.port, score));
                                }
                            }
                        }
                    }

                    // 3. DESPACHA APENAS O MICRO-LOTE
                    if let Some((worker_id, worker_ip, worker_port, score)) = melhor_worker {
                        println!(
                            "[SCHEDULER] Distribuindo lote de '{}' para o Worker '{}' ({}:{}) | Score: {:.1}/100",
                            id_tarefa, worker_id, worker_ip, worker_port, score
                        );
                        
                        let worker_url = format!("http://{}:{}", worker_ip, worker_port);
                        let fila_resgate = fila_sched.clone();
                        let id_resgate = id_tarefa.clone();
                        let comando_resgate = comando_lote.clone();

                        tokio::spawn(async move {
                            let conexao_grpc = WorkerServiceClient::connect(worker_url);
                            
                            match tokio::time::timeout(Duration::from_secs(3), conexao_grpc).await {
                                Ok(Ok(mut worker_client)) => {
                                    let task = Request::new(TaskRequest {
                                        task_id: id_tarefa,
                                        command: comando_lote,
                                        payload: vec![],
                                    });

                                    if let Ok(res) = worker_client.execute_task(task).await {
                                        let mut stream = res.into_inner();
                                        println!("\n=== LOGS DO MICRO-LOTE DE {} ===", worker_id);
                                        while let Ok(Some(out)) = stream.message().await {
                                            print!("{}", out.stdout);
                                        }
                                        println!("==============================================\n");
                                    }
                                }
_ => {
    eprintln!("[ALERTA] Falha no micro-lote de {}. Aplicando Backoff de segurança.", worker_id);
    
    // Pequena pausa para a rede não explodir em loop
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    let mut fila = fila_resgate.lock().unwrap();
    // ALTA ENGENHARIA: push_back joga pro FINAL da fila, matando o spam infinito no topo!
    fila.push_back(TarefaPendente {
        id: id_resgate,
        comando_base: "resgate".to_string(),
        sub_lotes: vec![comando_resgate],
        lotes_concluidos: 0,
    });
}

                            }
                        });
                    } else {
                        // Sem workers ativos? Devolve o lote fracionado para a fila
                        let mut fila = fila_sched.lock().unwrap();
                        fila.push_front(TarefaPendente {
                            id: id_tarefa,
                            comando_base: "resgate".to_string(),
                            sub_lotes: vec![comando_lote],
                            lotes_concluidos: 0,
                        });
                    }
                }
            });

            // TASK 3 - PROCESS THIEF (O LADRÃO DE PROCESSOS COM SUPORTE A SUB-LOTES)
            let fila_thief = master.fila_tarefas.clone();
            tokio::spawn(async move {
                let mut sys_thief = System::new_all();
                let processos_alvo = vec!["silverbench", "stress", "ffmpeg", "openssl"];

                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    
                    sys_thief.refresh_cpu_usage();
                    sys_thief.refresh_processes();

                    let cpu_mestre = sys_thief.global_cpu_info().cpu_usage();

                    if cpu_mestre > 90.0 {
                        for (pid, processo) in sys_thief.processes() {
                            let nome_processo = processo.name().to_lowercase();

                            for alvo in &processos_alvo {
                                if nome_processo.contains(alvo) {
                                    println!(
                                        "[PROCESS THIEF] CPU em {:.1}%! Interceptando processo local: '{}' (PID: {})",
                                        cpu_mestre, nome_processo, pid
                                    );

                                    processo.kill();
                                    println!("[PROCESS THIEF] Processo '{}' finalizado localmente.", nome_processo);

                                    // ALTA ENGENHARIA: O Thief transforma o comando interceptado em um lote processável!
                                    let mut fila = fila_thief.lock().unwrap();
                                    fila.push_back(TarefaPendente {
                                        id: format!("thief_{}_{}", nome_processo, pid),
                                        comando_base: nome_processo.clone(),
                                        sub_lotes: vec![nome_processo.clone()],
                                        lotes_concluidos: 0,
                                    });

                                    println!("[PROCESS THIEF] Lote assíncrono adicionado ao Pipeline do Cluster!");
                                    break;
                                }
                            }
                        }
                    }
                }
            });

            println!("[MASTER] Servidor gRPC escutando em {}", addr);
            Server::builder()
                .add_service(MasterServiceServer::new(master))
                .serve(addr)
                .await?;
        }

        Mode::Worker => {
            let worker_port = args.port + 1;
            let worker_addr = format!("0.0.0.0:{}", worker_port).parse()?;
            let worker_service = AetherWorker::default();

            println!("[WORKER] Servidor de tarefas escutando em {}", worker_addr);
            tokio::spawn(async move {
                if let Err(e) = Server::builder()
                    .add_service(WorkerServiceServer::new(worker_service))
                    .serve(worker_addr)
                    .await {
                    eprintln!("[WORKER] Falha crítica no servidor de tarefas: {}", e);
                }
            });

            let master_url = format!("http://{}:{}", args.master_ip, args.port);
            println!("[WORKER] Conectando ao Master em {}...", master_url);
            let mut client = MasterServiceClient::connect(master_url).await?;
            println!("[WORKER] Conectado!");

            let mut sys = System::new_with_specifics(
                RefreshKind::new().with_cpu(CpuRefreshKind::everything()),
            );
            let worker_id = std::fs::read_to_string("/etc/hostname")
                .unwrap_or_else(|_| "unknown_worker".to_string())
                .trim()
                .to_string();

            loop {
                sys.refresh_cpu_usage();
                sys.refresh_memory();

                let cpu_usage = sys.global_cpu_info().cpu_usage();
                let ram_usage = (sys.used_memory() as f32 / sys.total_memory() as f32) * 100.0;

                let components = Components::new_with_refreshed_list();
                let mut temperature = 0.0;
                for component in &components {
                    if component.label().to_lowercase().contains("cpu") || component.label().to_lowercase().contains("core") {
                        temperature = component.temperature();
                        break;
                    }
                }

                let request = Request::new(HeartbeatRequest {
                    worker_id: worker_id.clone(),
                    cpu_usage,
                    ram_usage,
                    temperature,
                    worker_port: worker_port as u32,
                });

                if let Err(e) = client.send_heartbeat(request).await {
                    eprintln!("[WORKER] Falha ao enviar heartbeat: {}", e);
                }

                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }

        Mode::Client => {
            if args.command.is_empty() {
                eprintln!("[ERRO CLI] Forneça um comando utilizando -c ou --command");
                std::process::exit(1);
            }

            let master_url = format!("http://{}:{}", args.master_ip, args.port);
            println!("[CLI] Conectando ao Master em {}...", master_url);
            
            let mut client = MasterServiceClient::connect(master_url).await?;
            
            let request = Request::new(TaskRequest {
                task_id: "manual_cli".to_string(),
                command: args.command.clone(),
                payload: vec![],
            });

            println!("[CLI] Injetando comando: '{}'...", args.command);
            
            match client.inject_task(request).await {
                Ok(resposta) => {
                    println!("[CLI] Sucesso! Resposta do Master: {}", resposta.into_inner().stdout.trim());
                    println!("[CLI] O Scheduler irá faturar o lote e direcionar em background.");
                }
                Err(e) => {
                    eprintln!("[CLI] Falha crítica ao injetar comando no Master: {}", e);
                }
            }
        }
    }
    Ok(())
}
