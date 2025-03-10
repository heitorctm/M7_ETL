from auxiliares import (
    formatar_colunas_data,
    remover_letras_coluna,
    truncar_2_casas,
    formatar_colunas_data,
    
)


from datetime import datetime
import pandas as pd

def t_diversificacao(dados):
    """
    Função que realiza as transformações específicas no dataframe.
    :param dados: DataFrame a ser transformado.
    :return: DataFrame transformado.
    """

    # Reorganiza as colunas na ordem desejada
    nova_ordem_colunas = [
        "Data",
        "Assessor",
        "Cliente",
        "Produto",
        "Sub Produto",
        "Produto em Garantia",
        "CNPJ Fundo",
        "Ativo",
        "Emissor",
        "Data de Vencimento",
        "Quantidade",
        "NET",
    ]
    dados = dados[nova_ordem_colunas]

    # Renomeia colunas
    dados = dados.rename(columns={
        "Data Posição": "data_ref",
        "Sub Produto": "Sub_Produto",
        "Produto em Garantia": "Produto_em_Garantia",
        "CNPJ Fundo": "CNPJ_Fundo",
        "Data de Vencimento": "Data_de_Vencimento"
    })
    
    # Converte CNPJ para string e remove .0
    dados['CNPJ_Fundo'] = dados['CNPJ_Fundo'].astype(str).str.replace('.0', '', regex=False)

    # Normaliza a coluna de data 'data_ref'
    dados['data_ref'] = pd.to_datetime(
        dados['data_ref'], 
        format='%d/%m/%Y',  # Ajuste para o formato correto
        errors='coerce'  # Transforma valores inválidos em NaT
    ).dt.strftime('%Y-%m-%d')  # Converte para o formato desejado

    # Remove letras de uma coluna específica
    dados = remover_letras_coluna(dados, coluna="Assessor")

    # Trunca valores em 2 casas decimais
    dados = truncar_2_casas(dados, colunas=["NET"])

    # Remover tabulações, quebras de linha, vírgulas e espaços das colunas "Ativo" e "Emissor"
    for coluna in ["Ativo", "Emissor"]:
        dados[coluna] = (
            dados[coluna]
            .astype(str)
            .str.replace(r'[\t\n\r,]', '', regex=True)  # Remove tabulações, quebras de linha e vírgulas
            .str.strip()  # Remove espaços no início e no fim
        )

    # Aplica tratamento para codificação de strings
    dados = dados.applymap(
        lambda x: (
            str(x).encode("latin-1", errors="ignore").decode("latin-1")
            if isinstance(x, str)
            else x
        )
    )

    # Formata a coluna de vencimento
    dados = formatar_colunas_data(
        dados, colunas_not_varchar=["Data_de_Vencimento"]
    )

    return dados

