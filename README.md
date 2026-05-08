# Real-Time Binance Data Pipeline
*Pipeline de dados distribuído para ingestão, processamento e armazenamento de eventos da API Binance em tempo real, utilizando Kafka e Spark Structured Streaming.*

## Arquitetura
- **Source:** Binance WebSocket API.
- **Ingestion:** Python Producer enviando para Kafka.
- **Streaming:** Spark Structured Streaming (Python) consumindo e tratando os dados.
- **Storage:** Arquivo parquet no AWS S3.

## Tecnologias utilizadas
- **Linguagem:** Python 3.13
- **Stream Processing**: Apache Spark 3.5.0
- **Message Broker:** Apache Kafka (Confluent)
- **Infraestrutura:** Docker / Docker Compose
- **Storage:** AWS S3

## Como Executar
```
# 1. Clone o repositório
git clone https://github.com/LucasHSantana/binance_pipeline_streaming_dados.git

# 2. No arquivo docker-compose.yaml: 
  2.1 No spark-consumer-app definir se SAVE_TO_S3 será false ou true.
  2.2 false não grava nada na aws s3
  2.3 true grava os dados na aws s3

# 3. Criar um arquivo .env na raíz do projeto com os dados:
AWS_ACCESS_KEY=<SUA_AWS_ACCESS_KEY>
AWS_SECRET_KEY=<SUA_AWS_SECRET_KEY>

# 4. Suba a infraestrutura (Kafka + Spark Cluster)
docker compose up -d
```

## Detalhes Técnicos e Decisões de Arquitetura
1. **Processamento Distribuído com Spark Standalone**  
   Para simular um ambiente de produção real, configurei um cluster Spark em modo Standalone via Docker, composto por:
   - **1 Spark Master:** Coordenador do plano de execução.
   - **3 Spark Workers:** Responsáveis pelo processamento paralelo das tarefas.

Esta arquitetura permitiu validar o escalonamento horizontal do pipeline, garantindo que o processamento dos trades da Binance dosse distribuído entre os nós.

2. **Integração Spark-Kafka**  
   Um dos maiores desafios técnicos foi a compatibilidade de versões e conectividade entre o cluster Spark e o Broker Kafka
   - **Resolução de dependências:** Implementei a inclusão dinâmica do pacote ```spark-sql-kafka-0-10``` para garantir que o Spark tivesse os conectores necessários para ler fluxos de dados binários.
   - **Serialização:** Realizei o parsing de dados binários vindos do Kafka para o formato estruturado JSON utilizando a função ```from_json``` do PySpark, permitindo consultas SQL sobre os dados em tempo real.

3. **Tolerância a Falhas e Checkpointing**  
   Em pipelines de streaming, a continuidade é muito importante. Por isso implementei o mecanismo de Checkpoint do Structured Streaming:
   -  **Garantia de Estado:** O uso do ```checkpointLocation``` permite que o pipeline salve o progresso em um armazenamento persistente.
   -  **Recuperação:** Em caso de falha de um worker ou reinicialização do sistema, o Spark retoma a leitura exatamente de onde parou no Kafka, evitando perda de dados ou duplicidade.

## Kafka producer
<img width="1909" height="1002" alt="kafka-producer" src="https://github.com/user-attachments/assets/c5adad3d-1ac9-4c84-a1b5-4bb44bfb426c" />

## Spark consumer
<img width="1915" height="641" alt="image" src="https://github.com/user-attachments/assets/85be9636-7764-4da2-8132-b35a6eb6bff2" />
<img width="1909" height="1002" alt="spark-consumer" src="https://github.com/user-attachments/assets/750cdd1c-601d-4df1-b7b8-8c43ace83e95" />


