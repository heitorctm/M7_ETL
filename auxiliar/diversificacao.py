from auxiliares import (
    truncar_2_casas,
    remover_linhas_sem_data,
    remover_letras_coluna,
    adicionando_aspas_duplas,
    formatar_colunas_data
)


def t_diversificacao(dados):
    """
    Função que realiza as transformações específicas no dataframe.
    :param dados: DataFrame a ser transformado.
    :return: DataFrame transformado.
    """
    # Reorganiza as colunas na ordem desejada
    nova_ordem_colunas = [
        "Data Posição",
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
    dados = dados.rename(columns={"Data Posição": "data_ref"})

    # Remove linhas sem data
    dados = remover_linhas_sem_data(dados)

    # Remove letras de uma coluna específica
    dados = remover_letras_coluna(dados, coluna="Assessor")

    # Trunca valores em 2 casas decimais
    dados = truncar_2_casas(dados, colunas=["NET"])

    # Aplica tratamento para codificação de strings
    dados = dados.applymap(
        lambda x: (
            str(x).encode("latin-1", errors="ignore").decode("latin-1")
            if isinstance(x, str)
            else x
        )
    )
    dados = formatar_colunas_data(dados)
    # Adiciona aspas duplas em colunas não varchar
    dados = adicionando_aspas_duplas(
        dados, colunas_not_varchar=["data_ref"]
    )

    return dados


def processar_tabela_diversificacao(dados):
    """
    Função principal que processa os dados da tabela.
    :param dados: DataFrame recebido diretamente.
    :return: DataFrame processado.
    """
    # Chama a função de processamento principal
    dados_processados = t_diversificacao(dados)
    return dados_processados
