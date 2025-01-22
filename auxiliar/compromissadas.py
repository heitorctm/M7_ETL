from .auxiliares import (
    acessar_s3,
    mover_arquivo,
    substituir_virgula_por_ponto,
    substituir_nan_por_zero,
    truncar_2_casas,
    remover_linhas_sem_data,
    remover_letras_coluna,
    formatar_colunas_data,
    adicionando_aspas_duplas,
    salvar_csv,
    carregar_arquivo,
    inserir_arquivo_no_s3,
)


def t_compromissada_s3(dados, path_do_arquivo):
    """
    tranform dos dados.
    1 - renomeia as colunas para o nome que tem no banco
    2 - remove as linhas de data ref que nao tem valores de datas.
        isso inclui as linhas de totais e as linhas que as vezes tem
        informacoes
    3 - remove as letras do codigo do assessor. as vezes o relatorio puxa
        o codigo com um 'A' na frente do codigo
    4 - coloca aspas duplas no cabecalho e dados da coluna. fiz para manter padrao
        nao sei a utilidade, provavelmente para garantir que os dados sejam inseridos
        com formato adequado 'varchar' no banco. por padrao, eu coloquei apenas a data_ref
        porem, podem ser adicionadas outras caso haja necessidade.(nesse caso, adicionei mais uma)
    5 - salva em formato CSV. o executavel so aceita rodar nest formato.

    retorna o caminho do CSV para o load.
    """

    novo_nome_colunas = [
        "data_ref",
        "data_venc",
        "cod_aai",
        "cod_xp",
        "posicao",
        "taxa",
        "receita",
    ]
    dados = dados.rename(columns=dict(zip(dados.columns, novo_nome_colunas)))
    dados = substituir_virgula_por_ponto(
        dados, colunas=["taxa"]
    )  # apenas para essa tabela
    dados = substituir_nan_por_zero(dados, coluna="posicao")
    dados = truncar_2_casas(dados, colunas=["receita", "posicao"])
    dados = substituir_nan_por_zero(dados, coluna="receita")
    dados = remover_linhas_sem_data(dados)
    dados = remover_letras_coluna(dados)
    dados = adicionando_aspas_duplas(
        dados, colunas_not_varchar=["data_ref", "data_venc"]
    )
    path = salvar_csv(dados, path_do_arquivo)

    return path


def load_compromissada_s3(
    path_do_arquivo, nome_base="compromissada", bucket="m7investimentos"
):
    pasta_destino = "../../BASE/9 - compromissada/processado/"
    dados = carregar_arquivo(path_do_arquivo)
    if dados is None:
        print("nao tem o arquivo")
        return True
    path = t_compromissada_s3(dados, path_do_arquivo)
    # mover_arquivo(path=path_do_arquivo, pasta_destino=pasta_destino)
    s3 = acessar_s3()
    inserir_arquivo_no_s3(nome_base, path, s3, bucket)
    # mover_arquivo(path=path, pasta_destino=pasta_destino)
