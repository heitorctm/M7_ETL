from auxiliares import (
    deletar_4_ultimas_colunas,
    remover_linhas_sem_data,
    remover_letras_coluna,
    adicionando_aspas_duplas,
    remover_linhas_nan,
    formatar_colunas_data,
    remover_linhas_nan,
)


def t_analitico_rf(dados):
    """
    Função que realiza as transformações específicas no dataframe.
    :param dados: DataFrame a ser transformado.
    :return: DataFrame transformado.
    """
    # Define os campos e reorganiza as colunas
    campos = [
        "Date",
        "Código Assessor",
        "Código Conta",
        "Classificação Ativo N3",
        "Produto N3",
        "Ticker",
        "Nome Papel",
        "Data Vencimento",
        "Tipo Operação",
        "Financeiro Transacionado",
        "Receita a Dividir",
        "PU Cliente",
        "PU TMR",
        "Taxa Cliente",
        "Taxa TMR",
        "Indexador",
        "Quantidade Operação",
    ]
    dados = dados[campos]

    # Renomeia as colunas
    novo_nome_colunas = [
        "data_ref",
        "cod_aai",
        "cod_xp",
        "classificacao",
        "produto",
        "ticker",
        "nome_papel",
        "data_vencimento",
        "tipo",
        "volume",
        "receita",
        "preco_unitario",
        "preco_tmr",
    ]
    dados = dados.rename(columns=dict(zip(dados.columns, novo_nome_colunas)))
    dados = remover_linhas_nan(dados,'cod_xp')
    # Remove linhas sem data e colunas nulas
    dados = remover_linhas_sem_data(dados)
    dados = remover_linhas_nan(dados, coluna="cod_xp")

    # Remove letras de colunas específicas
    dados = remover_letras_coluna(dados)

    # Adiciona aspas duplas em colunas não varchar
    dados = formatar_colunas_data(dados)
    dados = adicionando_aspas_duplas(dados, colunas_not_varchar=["data_ref"])

    # Remove as 4 últimas colunas
    dados = deletar_4_ultimas_colunas(dados)

    return dados


def processar_tabela_analitico_rf(dados):
    """
    Função principal que processa os dados da tabela.
    :param dados: DataFrame recebido diretamente.
    :return: DataFrame processado.
    """
    # Chama a função de processamento principal
    dados_processados = t_analitico_rf(dados)
    return dados_processados
