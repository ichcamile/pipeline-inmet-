import requests
import zipfile
import io
import os
import pandas as pd

YEAR = "2024"

URL_DADOS = f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{YEAR}.zip"

CAMINHO_BRONZE = f"data/input/inmet/{YEAR}"

os.makedirs(CAMINHO_BRONZE, exist_ok=True)

print(f"Estrutura de pastas garantida em: {CAMINHO_BRONZE}")


def download_data():
    print(f"Baixando os Dados do INMET para o ano: {YEAR}")

    response = requests.get(URL_DADOS, timeout=60)

    if response.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(CAMINHO_BRONZE)

        print("Download concluído")
        return True

    else:
        print(f"Erro ao baixar os dados: {response.status_code}")
        return False


download_data()

lista_arquivos = os.listdir(CAMINHO_BRONZE)

arquivos = [f for f in lista_arquivos if f.lower().endswith(".csv")]

dataframes = []

for arquivo in arquivos:

    caminho_csv = os.path.join(CAMINHO_BRONZE, arquivo)

    # leitura metadados
    with open(caminho_csv, "r", encoding="latin1") as f:
        cabecalho_linhas = [next(f).strip() for _ in range(8)]

    metadados = {}

    for linha in cabecalho_linhas:
        partes = linha.split(":", 1)

        if len(partes) == 2:
            metadados[partes[0].strip()] = partes[1].strip()

    # leitura da tabela
    df = pd.read_csv(
        caminho_csv,
        sep=";",
        encoding="latin1",
        skiprows=8
    )

    # adicionar metadados
    for chave, valor in metadados.items():
        df[chave] = valor

    dataframes.append(df)

# juntar todos os CSV
df_final = pd.concat(dataframes, ignore_index=True)

print(df_final.head())