## Modelagem e Processamento de Dados - Hilab

Este código foi escrito em Python 3.10 para solucionar o exercício 2. Siga as etapas abaixo para executá-lo:

1. Certifique-se de ter o Python (versão 3.9 ou superior) e o pip instalados. Para isso, pode rodar as seguintes linhas de código:
    ```
    python3 --version
    pip3 --version
    ```

2. Crie um novo ambiente virtual usando o seguinte comando:

    ```
    python3.10 -m venv .venv
    ```

3. Ative o ambiente virtual:

    ```
    source .venv/bin/activate
    ```

4. Em seguida, instale todas as bibliotecas necessárias com o seguinte comando:

    ```
    pip3 install -r requirements.txt
    ```

5. Foi utilizado o banco de dados PostgreSQL. Para autenticar com o mesmo, é necessário criar um arquio `.env` no _root (/)_ do diretório. Para isso, rode o comando:
    ```
    touch .env
    ```


6. Feito isso, abra o arquivo e insira as credenciais conforme:
    ```
    dbname=''
    user=''
    host=''
    password=''
    ```
    Adicionando os valores dentro das aspas simples. 
    Como padrão, o nome do banco de dados utilizado foi `database_Hilab`. Certifique-se de criar uma base de dados com esse nome e utilize o usuário/senha de sua preferência. Como a execução é local, o host pode ser definido como `localhost`.

7. Para rodar o script, execute o seguinte comando:

    ```    
    python3.10 main.py
    ```

Este comando irá criar o esquema, as tabelas, processar os arquivos .csv e inserir os dados no banco de dados!
