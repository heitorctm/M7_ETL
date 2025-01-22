from .auxiliares import (
    acessar_s3,
    mover_arquivo,
    remover_linhas_sem_data,
    remover_letras_coluna,
    formatar_colunas_data,
    adicionando_aspas_duplas,
    salvar_csv,
    carregar_arquivo,
    inserir_arquivo_no_s3,
)


def t_fluxo_corretagem_s3(dados, path_do_arquivo):

    colunas_nova_ordem = [
        "Data",
        "Cod A",
        "Conta",
        "Suitability",
        "Matriz",
        "Ativo",
        "Qtd",
        "Corretagem",
        "Volume Negociado",
        "Produto",
        "Canal",
        "Tipo de Corretagem",
        "Mercado",
        "Lado",
    ]

    dados = dados[colunas_nova_ordem]

    novo_nome_colunas = [
        "data_ref",
        "cod_aai",
        "cod_xp",
        "suitability",
        "matriz",
        "ativo",
        "qtd",
        "receita",
        "volume",
        "produto",
        "canal",
        "tipo",
        "mercado",
        "lado",
    ]
    dados = dados.rename(columns=dict(zip(dados.columns, novo_nome_colunas)))
    dados = remover_linhas_sem_data(dados, colunas=["data_ref"])
    dados = remover_letras_coluna(dados)
    dados = adicionando_aspas_duplas(dados, colunas_not_varchar=["data_ref"])
    path = salvar_csv(dados, path_do_arquivo)

    return path


def load_fluxo_corretagem_s3(
    path_do_arquivo, nome_base="fluxo_corretagem", bucket="m7investimentos"
):

    pasta_destino = "../../BASE/10 - fluxo_corretagem/processado/"
    dados = carregar_arquivo(path_do_arquivo)
    if dados is None:
        print("nao tem o arquivo")
        return True
    path = t_fluxo_corretagem_s3(dados, path_do_arquivo)
    # mover_arquivo(path=path_do_arquivo, pasta_destino=pasta_destino)
    s3 = acessar_s3()
    inserir_arquivo_no_s3(nome_base, path, s3, bucket)
    # mover_arquivo(path=path, pasta_destino=pasta_destino)
