from auxiliares import (
    consulta_bc,
    truncar_2_casas,
    remover_linhas_sem_data,
    remover_letras_coluna,
    formatar_colunas_data,
    adicionando_aspas_duplas,
)


def t_xpus_cap(dados):
    """
    Realiza as transformações específicas no DataFrame da tabela `xpus_cap`.

    :param dados: DataFrame a ser transformado.
    :return: DataFrame transformado.
    """
    # Define a nova ordem das colunas
    nova_ordem_colunas = [
        "Date",
        "Código da Conta Offshore PWM",
        "Código da Conta Brasil",
        "Tipo de Operação",
        "Volume Financeiro Movimentado ($)",
        "Código do Assessor",
    ]

    dados = dados[nova_ordem_colunas]

    # Renomeia as colunas
    novo_nome_colunas = ["data_ref", "cod_us", "cod_xp", "tipo", "captacao", "cod_aai"]
    dados = dados.rename(columns=dict(zip(dados.columns, novo_nome_colunas)))

    # Remove linhas sem data
    dados = remover_linhas_sem_data(dados)

    # Formata a coluna de data
    dados = formatar_colunas_data(dados, colunas_not_varchar=["data_ref"])

    # Consulta o fator do Banco Central e ajusta os valores de captação
    dados["Fator BC"] = dados["data_ref"].apply(
        lambda date: consulta_bc(date.strftime("%d/%m/%Y"))
    )
    dados["captacao"] = pd.to_numeric(dados["captacao"], errors="coerce")
    dados["captacao"] = dados["captacao"] * dados["Fator BC"]

    # Remove a coluna auxiliar "Fator BC"
    dados = dados.drop(columns=["Fator BC"])

    # Remove letras da coluna de códigos e trunca valores
    dados = remover_letras_coluna(dados, coluna="cod_aai")
    dados = truncar_2_casas(dados, colunas=["captacao"])

    # Adiciona aspas duplas às colunas especificadas
    dados = adicionando_aspas_duplas(dados, colunas_not_varchar=["data_ref"])

    return dados


def processar_tabela_xpus_cap(dados):
    """
    Função principal que processa os dados da tabela `xpus_cap`.

    :param dados: DataFrame recebido diretamente do nó KNIME.
    :return: DataFrame processado.
    """
    # Chama a função de processamento principal
    dados_processados = t_xpus_cap(dados)
    return dados_processados
