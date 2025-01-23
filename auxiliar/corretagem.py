from auxiliares import (
    truncar_2_casas,
    remover_linhas_sem_data,
    remover_letras_coluna,
    formatar_colunas_data,
    adicionando_aspas_duplas,
)


def t_corretagem(dados):
    """
    Realiza as transformações específicas no DataFrame da tabela `corretagem`.

    :param dados: DataFrame a ser transformado.
    :return: DataFrame transformado.
    """
    dados = dados.astype(str)
    # Adiciona coluna 'data_ref' e reorganiza a ordem das colunas
    dados["data_ref"] = dados["Data"]
    nova_ordem_colunas = [
        "data_ref",
        "Conta",
        "Data",
        "BMF",
        "BOV",
        "Total",
        "Cod Matriz",
        "Cod A",
        "Tipo Corretagem",
        "Canal",
    ]
    dados = dados[nova_ordem_colunas]
    dados = dados.astype(str)

    # Aplica as transformações
    dados = remover_linhas_sem_data(dados)
    dados = truncar_2_casas(dados, colunas=["BOV", "Total"])
    
    

    return dados


def processar_tabela_corretagem(dados):
    """
    Função principal que processa os dados da tabela `corretagem`.

    :param dados: DataFrame recebido diretamente do nó KNIME.
    :return: DataFrame processado.
    """
    # Chama a função de processamento principal
    dados_processados = t_corretagem(dados)
    return dados_processados
