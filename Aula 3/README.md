# Prática: Containers Docker
Esta é a prática sugerida para o instrutor aplicar à turma na 3ª aula do curso de Engenharia de Dados da Awari.

A ideia é fazer o download do Docker e do repositório com os containers que serão utilizados todo o curso, bem como rodar o docker-compose para montar o workspace com aplicações, práticas, exercícios e muito mais.

Para isso, o passo a passo é o seguinte — as etapas também estão instruídas nos slides da aula:

1. Fazer download do Docker Compose;

2. Orientar a turma durante a instalação;

3. Fazer download do repositório no Github com a imagem Docker que será utilizada no curso;

4. Executar docker-compose up na pasta em que foi baixada a imagem;

5. Aguardar a execução;

6. Explorar junto da turma o que o workspace oferece e explicar o que ocorreu nos bastidores — o ambiente montado permite desenvolvimento em Python no JupyterLab Server, uso de Apache Kafka, Apache Spark com PySpark, Airflow, MongoDB, PostgreSQL, além de contar com rotinas e automatizações nos bastidores. Sugere-se, especificamente, verificar com a turma:

  6.1 se o docker-compose está funcionando;
  
  6.2 se as imagens estão “up”, além de fazer uma conexão SSH usando o comando de bash em uma imagem docker;
  deletar e criar de novo os containers;
  
  6.3 verificar arquivos gerados na pasta /data (binários dos containers).
  
  6.4 Replicar o trabalho com Docker, para fixar instruções à turma, a partir de imagens disponíveis no Docker Hub.
  
  6.5 O instrutor tem a liberdade de adaptar e incrementar a prática, visando destacar a importância do Docker e ensinar a utilizá-lo.
