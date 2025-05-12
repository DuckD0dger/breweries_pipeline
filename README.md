# Breweries Case Data Pipeline

## Descrição
Este projeto é uma solução para o case de engenharia de dados da BEES. Ele extrai dados da API Open Brewery DB, transforma-os em uma arquitetura de data lake em camadas (bronze, silver, gold) e agrega informações analíticas.

## Como Executar

1. **Pré-requisitos**:
   - Docker e Docker Compose instalados.

2. **Configurar e iniciar o ambiente**:
```bash
git clone <este-repositório>
cd project
docker-compose up --build
```

3. **Acessar o Airflow**:
- URL: [http://localhost:8080](http://localhost:8080)
- Usuário: `admin`
- Senha: `admin`

4. **Executar o DAG**:
- Ative e dispare manualmente o DAG `breweries_pipeline`.

## Estrutura do Projeto
- `dags/`: DAG do Airflow.
- `scripts/`: Scripts de extração, transformação e agregação.
- `data/`: Armazenamento em camadas bronze, silver e gold.
- `tests/`: Testes unitários com Pytest.

## Monitoramento
- Para monitorar falhas ou lentidões no pipeline, o Airflow fornece logs e alertas por e-mail podem ser adicionados em produção.

## Testes
Para rodar os testes:
```bash
docker exec -it <container_airflow> pytest tests/
```

## Transformações
- Silver: remove colunas irrelevantes e registros sem estado.
- Gold: agrega o número de cervejarias por tipo e estado.
