import pandas as pd
from sqlalchemy import create_engine

# Configuração do banco de dados de destino (Change as configurações de acordo com seu ambiente)
db_engine = create_engine('postgresql://username:password@localhost/mydatabase')

# 1. Extração: Ler dados de uma fonte (por exemplo, um arquivo CSV)
def extract_data():
    data = pd.read_csv('dados.csv')
    return data

# 2. Transformação: Aplicar transformações nos dados
def transform_data(data):
    # Exemplo de transformação simples: converter uma coluna para maiúsculas
    data['Nome'] = data['Nome'].str.upper()
    return data

# 3. Carregamento: Carregar dados transformados no banco de dados
def load_data(data, db_engine):
    data.to_sql('tabela_destino', db_engine, if_exists='replace', index=False)

if __name__ == "__main__":
    # Executar a pipeline ETL
    data = extract_data()
    data_transformed = transform_data(data)
    load_data(data_transformed, db_engine)
    print("Pipeline ETL concluída.")

