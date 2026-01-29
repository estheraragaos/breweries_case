# Open Brewery DB Pipeline - Medallion Architecture

Este projeto implementa um pipeline de dados automatizado para consumir, transformar e agregar dados da API **Open Brewery DB**. A solução utiliza a **Arquitetura Medallion** (Bronze, Silver e Gold) com processamento distribuído em **PySpark** e armazenamento em **Delta Lake**, tudo orquestrado pelo **Apache Airflow**.

## Arquitetura e Decisões de Design

Desenhei a solução para ser escalável e resiliente, utilizando as seguintes camadas:

* **Bronze Layer:** Persisto os dados brutos da API em formato JSON, garantindo a imutabilidade e a linhagem do dado original. A ingestão conta com tratamento de erros e retries.
* **Silver Layer (Curated):** Realizo a limpeza e padronização (normalização de strings, remoção de espaços e tipagem). Converto os dados para **Delta Lake** e aplico o **particionamento por país (`country`)**, otimizando consultas geográficas.
* **Gold Layer (Analytical):** Camada final onde consolido a quantidade de cervejarias por tipo e localização, fornecendo uma visão pronta para consumo analítico.

### Escolhas Técnicas e Trade-offs:
* **Delta Lake vs Parquet:** Escolhi o Delta Lake pela necessidade de transações ACID e *Schema Enforcement*. Isso garante que dados corrompidos não cheguem às camadas finais. Utilizo `overwriteSchema` para permitir evoluções controladas no contrato de dados.
* **Eficiência de Armazenamento:** Na camada Gold, aplico o `.coalesce(1)` antes da escrita para mitigar o *Small File Problem*. Isso reduz o overhead de metadados e acelera a leitura por ferramentas de visualização (BI).
* **Containerização:** Isolei o projeto 100% via Docker, garantindo que dependências complexas (Java, Spark JARs e bibliotecas Python) funcionem de forma idêntica em qualquer máquina.

## Tecnologias Utilizadas

* **Orquestração:** Apache Airflow
* **Processamento:** PySpark (Spark 3.9+)
* **Armazenamento:** Delta Lake (2.4.0)
* **Infraestrutura:** Docker & Docker Compose

## Como Executar

### 1. Instalação do Docker
Caso não possua o Docker instalado, utilize os comandos abaixo (exemplo para distribuições baseadas em Debian/Ubuntu):

```bash
# Atualizar repositórios e instalar dependências
sudo apt-get update
sudo apt-get install ca-certificates curl gnupg
```
# Adicionar chave oficial do Docker e instalar Engine + Compose
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin\

*Para Windows ou macOS, recomendo o [Docker Desktop](https://www.docker.com/products/docker-desktop/).*

### 2. Configuração do Projeto
1. Clone o repositório e navegue até a pasta:
   ```bash
   git clone [https://github.com/estheraragaos/breweries_case.git](https://github.com/estheraragaos/breweries_case.git)
   cd breweries_case
   ```
2. Suba o ambiente Airflow/Spark:
  ```bash
   docker compose up -d
   ```
3. Acesse o Airflow em [http://localhost:8080](http://localhost:8080) (Login: **airflow** | Senha: **airflow**).
4. Ative a DAG `brewery_pipeline` para iniciar o fluxo.

## Suíte de Testes

Valido a confiabilidade do código via **pytest**. Os testes cobrem a integração entre as camadas e a precisão da lógica de agregação.

Para rodar os testes no container:
```bash
docker exec -it <container_id_do_worker> python -m pytest /opt/airflow/tests/
```
## Monitoramento e Observabilidade

A estratégia de monitoramento proposta vai além da verificação de falhas, focando na saúde do dado e eficiência do pipeline:

1. **Pipeline Health & SLA:**
    * **Monitoramento de Latência de Dados:** Alertas configurados não apenas para falhas, mas para atrasos que comprometam o SLA de entrega (ex: dados da Gold atrasados em +1h).
    * **Integração de Slack Alerts:** Uso de callbacks do Airflow para notificação imediata de erros críticos em canais de comunicação.

2. **Data Quality & Drift:**
    * **Great Expectations:** Validação de contratos de dados (schema, nulos, unicidade) antes da promoção para a camada Gold.
    * **Monitoramento de Data Drift:** Alertas caso a volumetria de ingestão varie drasticamente (ex: queda de 50% nos registros), indicando possíveis anomalias na API de origem.

3. **Performance Spark:**
    * **Spark UI:** Acompanhamento de métricas de *Spill to Disk* e *Skewness* (desbalanceamento de partições) para garantir o dimensionamento correto do cluster e evitar desperdício de recursos.

## ☁️ Estratégia de Escalabilidade em Nuvem (Azure)

Embora esta solução seja executada localmente via Docker para garantir reprodutibilidade, a arquitetura foi desenhada para ser migrada para o ecossistema **Microsoft Azure** com ajustes mínimos:

1. **Storage (Data Lake):**
    * **Local:** Sistema de arquivos do container.
    * **Azure:** **Azure Data Lake Storage Gen2 (ADLS Gen2)**.
    * **Migração:** Alteração do esquema de escrita de `file://` para `abfss://<container>@<account>.dfs.core.windows.net/`.

2. **Compute (Spark Processing):**
    * **Local:** Container Spark Standalone.
    * **Azure:** **Azure Databricks**.
    * **Justificativa:** O Databricks oferece o ambiente otimizado para Delta Lake (Photon Engine) e integração nativa com o Microsoft Entra ID (antigo Active Directory).

3. **Gerenciamento de Segredos:**
    * Para garantir a segurança das credenciais (API Keys), em produção utilizaria o **Azure Key Vault**. As chaves seriam injetadas como segredos no cluster Databricks (Secret Scopes) ou no Airflow, eliminando qualquer credencial *hardcoded* no código.

## Visão de Escopo Futuro

Para evoluir o projeto para um cenário de Big Data produtivo e governado:

* **Infraestrutura como Código (IaC):** Gerenciar o provisionamento de recursos (Resource Groups, Storage Accounts, Clusters) via **Terraform**, garantindo um ambiente imutável e versionado.
* **Otimização de Storage (Delta Lake):** Implementar rotinas de manutenção como `VACUUM` (limpeza de arquivos antigos) e `OPTIMIZE ZORDER BY` para acelerar consultas filtradas pela coluna de particionamento (`country`).
* **Data Governance:** Implementação de um catálogo de dados (ex: **DataHub** ou **Azure Purview**) para expor a linhagem dos dados (*Data Lineage*) e dicionário de métricas para os consumidores de negócio.
* **CI/CD Avançado:** Pipeline de deploy automatizado que roda testes unitários, integração e análise estática de código (Linter/SonarQube) antes do merge.
