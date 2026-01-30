"""
Script para sincronizar dados do Supabase e salvar como CSV no repositório.
Este script é executado pelo GitHub Actions.
"""

import os
import pandas as pd
from supabase import create_client

# ===============================
# Configurações do Supabase
# ===============================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABELA_LOG = os.getenv("SUPABASE_TABELA_LOG", "log_of_nf")

# Nome do arquivo CSV
CSV_FILE = "dados_supabase.csv"


# ===============================
# Funções auxiliares
# ===============================
def extrair_info_volume(chave: str):
    """
    Extrai informações de volume da chave da NF.
    Retorna (base, vol_atual, total_vol) ou None.
    """
    if not chave or len(chave) < 6:
        return None

    sufixo = chave[-6:]
    if not sufixo.isdigit():
        return None

    vol_atual = int(sufixo[:3])
    total_vol = int(sufixo[3:])
    base = chave[:-6]

    return base, vol_atual, total_vol


def extrair_nf_cnpj(chave: str):
    """Extrai o número da NF e o CNPJ da chave."""
    if not chave or len(chave) < 26:
        return None, None

    nf = chave[:9]
    cnpj = chave[12:26]

    if not nf.isdigit() or not cnpj.isdigit():
        return None, None

    return nf, cnpj


def formatar_cnpj(cnpj: str) -> str:
    """Formata CNPJ no padrão XX.XXX.XXX/XXXX-XX."""
    if not cnpj or len(cnpj) != 14:
        return "N/A"

    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"


# ===============================
# Extração do Supabase (SEM deduplicação)
# ===============================
def carregar_dados_supabase(client, incluir_removidas: bool = True):
    """
    Carrega todos os dados do Supabase usando paginação.
    NÃO remove duplicatas.
    """
    print(f"  Tentando acessar tabela: {SUPABASE_TABELA_LOG}")

    all_data = []
    page_size = 1000
    offset = 0
    total_loaded = 0

    while True:
        query = client.table(SUPABASE_TABELA_LOG).select("*")

        if not incluir_removidas:
            query = query.is_("removido_em", "null")

        # Tentativa de ordenação segura
        for campo_ordenacao in ["bipado_em", "CHAVE_NF", "OF"]:
            try:
                query = query.order(campo_ordenacao, desc=False)
                break
            except Exception:
                continue

        inicio = offset
        fim = offset + page_size - 1
        print(f"  Buscando registros {inicio} a {fim}...")

        resp = query.range(inicio, fim).execute()

        if not resp.data:
            break

        for registro in resp.data:
            all_data.append(registro)

        total_loaded += len(resp.data)
        print(f"  Carregados {total_loaded} registros")

        if len(resp.data) < page_size:
            break

        offset += page_size
        if offset > 1_000_000:
            print("  Limite de segurança atingido")
            break

    print(f"\n  Total de {len(all_data)} registros carregados.")
    return pd.DataFrame(all_data)


# ===============================
# Enriquecimento dos dados
# ===============================
def enriquecer_dados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquece o DataFrame com informações extraídas da CHAVE_NF.
    """
    if df.empty:
        return df

    dados_enriquecidos = []

    for _, row in df.iterrows():
        chave = row.get("CHAVE_NF", "")

        if isinstance(chave, str) and len(chave) == 48:
            nf, cnpj = extrair_nf_cnpj(chave)
            info_vol = extrair_info_volume(chave)
        else:
            nf, cnpj, info_vol = None, None, None

        dados_enriquecidos.append({
            "OF": row.get("OF"),
            "CHAVE_NF": chave,
            "NF": nf or "N/A",
            "CNPJ": cnpj or "N/A",
            "CNPJ_Formatado": formatar_cnpj(cnpj),
            "Volume_Atual": info_vol[1] if info_vol else None,
            "Total_Volumes": info_vol[2] if info_vol else None,
            "Base_NF": info_vol[0] if info_vol else None,
            "Bipado_em": row.get("bipado_em"),
            "Removido_em": row.get("removido_em"),
            "Inclusao_Manual": row.get("INCLUSAO_MANUAL") is True,
        })

    return pd.DataFrame(dados_enriquecidos)


# ===============================
# Main
# ===============================
def main():
    print("=" * 60)
    print("Sincronização Supabase -> CSV")
    print("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERRO: SUPABASE_URL e SUPABASE_KEY devem estar configurados!")
        exit(1)

    print("\nConectando ao Supabase...")
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("✓ Conexão estabelecida!")

    print("\nCarregando dados...")
    df = carregar_dados_supabase(client, incluir_removidas=True)

    if df.empty:
        pd.DataFrame().to_csv(CSV_FILE, index=False)
        print("CSV vazio criado.")
        return

    print("\nEnriquecendo dados...")
    df_enriquecido = enriquecer_dados(df)

    print(f"\nSalvando CSV: {CSV_FILE}")
    df_enriquecido.to_csv(CSV_FILE, index=False, encoding="utf-8-sig")

    print("✓ Processo concluído com sucesso!")


if __name__ == "__main__":
    main()
