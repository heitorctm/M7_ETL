from .auxiliares import (
    acessar_s3,
    salvar_csv,
    carregar_arquivo,
    inserir_arquivo_no_s3,
    remover_hifen,
    adicionando_aspas_duplas,
    formatar_colunas_data_transf,
)


def t_processamentos_s3(dados, path_do_arquivo):

    colunas_a_remover = [
        "Nome Assessor Origem",
        "Nome Assessor Destino",
        "Origem Solicitação",
        "Código Solicitação",
    ]
    dados = dados.drop(columns=colunas_a_remover)
    print(dados.head(3))

    novos_nomes = [
        "cod_xp",
        "cod_aai",
        "cod_aai_destinho",
        "data_solicitacao",
        "data_transferencia",
        "status",
    ]
    dados.columns = novos_nomes

    dados = formatar_colunas_data_transf(
        dados, colunas_not_varchar=["data_solicitacao"]
    )
    dados = formatar_colunas_data_transf(
        dados, colunas_not_varchar=["data_transferencia"]
    )

    dados = remover_hifen(dados, ["cod_aai", "cod_aai_destinho"])
    dados = adicionando_aspas_duplas(dados, ["none"])

    path = salvar_csv(dados, path_do_arquivo)

    return path


def load_processamentos_s3(
    path_do_arquivo, nome_base="tabela_processamentos", bucket="m7investimentos"
):

    pasta_destino = "../../BASE/11 - tabela_processamentos/processado/"
    dados = carregar_arquivo(path_do_arquivo)
    if dados is None:
        print("nao tem o arquivo")
        return True
    path = t_processamentos_s3(dados, path_do_arquivo)
    # mover_arquivo(path=path_do_arquivo, pasta_destino=pasta_destino)
    s3 = acessar_s3()
    inserir_arquivo_no_s3(nome_base, path, s3, bucket)
    # mover_arquivo(path=path, pasta_destino=pasta_destino)
