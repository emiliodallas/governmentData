## Modelagem e Processamento de Dados - Hilab

Este código foi escrito em Python 3.10 para solucionar o exercício 2. Siga as etapas abaixo para executá-lo:

1. Certifique-se de ter o Python (versão 3.9 ou superior) e o pip instalados.

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

Foi utilizado o banco de dados PostgreSQL com os seguintes parâmetros:

- Nome do banco de dados: `database_Hilab`
- Usuário: `postgre`
- Senha: `123`
- Host: `localhost`

Esses dados devem ser atualizados no arquivo `main.py` nas chamadas das classes `DataProcessor` e `DatabaseManager`.

Certifique-se de criar uma base de dados com esse nome e utilize o usuário/senha de sua preferência. Como a execução é local, o host pode ser definido como `localhost`.

5. Para rodar o script, execute o seguinte comando:

    ```    
    python3.10 main.py
    ```

Este comando irá criar o esquema, as tabelas, processar os arquivos .csv e inserir os dados no banco de dados!
