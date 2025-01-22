from .auxiliares import (
    acessar_s3,
    truncar_2_casas,
    mover_arquivo,
    remover_linhas_sem_data,
    remover_letras_coluna,
    adicionando_aspas_duplas_positivador,
    salvar_csv,
    carregar_arquivo,
    inserir_arquivo_no_s3,
)


def t_diversificacao_s3(dados, path_do_arquivo):

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
    dados = dados.rename(columns={"Data Posição": "data_ref"})
    dados["teste"] = 1
    dados = remover_linhas_sem_data(dados)

    dados = remover_letras_coluna(dados, coluna="Assessor")
    dados = truncar_2_casas(dados, colunas=["NET"])
    dados = dados.applymap(
        lambda x: (
            str(x).encode("latin-1", errors="ignore").decode("latin-1")
            if isinstance(x, str)
            else x
        )
    )
    dados = adicionando_aspas_duplas_positivador(
        dados, colunas_not_varchar=["data_ref"]
    )
    path = salvar_csv(dados, path_do_arquivo)

    return path


def load_diversificacao_s3(
    path_do_arquivo, nome_base="diversificacao", bucket="m7investimentos"
):

    pasta_destino = "../../BASE/0.1 - diversificacao/processado/"
    dados = carregar_arquivo(path_do_arquivo)
    if dados is None:
        print("nao tem o arquivo")
        return True
    path = t_diversificacao_s3(dados, path_do_arquivo)
    # mover_arquivo(path=path_do_arquivo, pasta_destino=pasta_destino)
    s3 = acessar_s3()
    inserir_arquivo_no_s3(nome_base, path, s3, bucket)
    # mover_arquivo(path=path, pasta_destino=pasta_destino)
