from .auxiliares import (
    acessar_s3,
    deletar_4_ultimas_colunas,
    mover_arquivo,
    remover_linhas_sem_data,
    remover_letras_coluna,
    formatar_colunas_data,
    adicionando_aspas_duplas,
    salvar_csv,
    carregar_arquivo,
    inserir_arquivo_no_s3,
    remover_linhas_nan,
)


def t_analitico_rf_s3(dados, path_do_arquivo):
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
    dados = remover_linhas_sem_data(dados)
    dados = remover_linhas_nan(dados, coluna="cod_xp")
    dados = remover_letras_coluna(dados)
    dados = adicionando_aspas_duplas(dados, colunas_not_varchar=["data_ref"])
    dados = deletar_4_ultimas_colunas(dados)
    path = salvar_csv(dados, path_do_arquivo)

    return path


def load_analitico_rf_s3(
    path_do_arquivo, nome_base="analitico_rf", bucket="m7investimentos"
):

    pasta_destino = "../../BASE/8 - analitico/processado/"
    dados = carregar_arquivo(path_do_arquivo)
    if dados is None:
        print("nao tem o arquivo")
        return True
    path = t_analitico_rf_s3(dados, path_do_arquivo)
    # mover_arquivo(path=path_do_arquivo, pasta_destino=pasta_destino)
    s3 = acessar_s3()
    inserir_arquivo_no_s3(nome_base, path, s3, bucket)
    # mover_arquivo(path=path, pasta_destino=pasta_destino)
