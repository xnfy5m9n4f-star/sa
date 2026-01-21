"""
Script para sincronizar dados do Supabase e salvar como CSV no repositório.
Este script é executado pelo GitHub Actions.
"""

import os
import pandas as pd
from supabase import create_client

# Configurações do Supabase (vindas de variáveis de ambiente/secrets)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABELA_LOG = os.getenv("SUPABASE_TABELA_LOG", "log_of_nf")

# Nome do arquivo CSV a ser salvo
CSV_FILE = "dados_supabase.csv"


def extrair_info_volume(chave: str):
    """
    Extrai informações de volume da chave da NF.
    Retorna (base, vol_atual, total_vol) ou None.
    """
    if len(chave) < 6:
        return None
    sufixo = chave[-6:]
    if not sufixo.isdigit():
        return None
    vol_atual = int(sufixo[:3])
    total_vol = int(sufixo[3:])
    base = chave[:-6]
    return base, vol_atual, total_vol


def extrair_nf_cnpj(chave: str):
    """Extrai o número da NF e CNPJ da chave."""
    if len(chave) < 26:
        return None, None
    nf = chave[:9]
    cnpj = chave[12:26]
    if not nf.isdigit() or not cnpj.isdigit():
        return None, None
    return nf, cnpj


def formatar_cnpj(cnpj: str) -> str:
    """Formata CNPJ no padrão XX.XXX.XXX/XXXX-XX."""
    if not cnpj or len(cnpj) != 14:
        return cnpj
    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"


def carregar_dados_supabase(client, incluir_removidas: bool = True):
    """
    Carrega todos os dados do Supabase usando paginação.
    O Supabase tem limite padrão de 1000 registros por consulta,
    então implementamos paginação para buscar todos os dados.
    
    Args:
        client: Cliente do Supabase
        incluir_removidas: Se True, inclui registros removidos
    
    Returns:
        DataFrame com os dados
    """
    print(f"  Tentando acessar tabela: {SUPABASE_TABELA_LOG}")
    
    try:
        all_data = []
        page_size = 1000  # Tamanho máximo por página do Supabase
        offset = 0
        total_loaded = 0
        seen_keys = set()  # Para detectar duplicatas
        
        while True:
            # Monta a query base
            query = client.table(SUPABASE_TABELA_LOG).select("*")
            
            if not incluir_removidas:
                query = query.is_("removido_em", "null")
            
            # IMPORTANTE: Ordenação é necessária para paginação consistente
            # Tenta ordenar por diferentes campos para garantir ordem estável
            ordenado = False
            for campo_ordenacao in ["bipado_em", "CHAVE_NF", "OF"]:
                try:
                    query = query.order(campo_ordenacao, desc=False)
                    ordenado = True
                    break
                except:
                    continue
            
            if not ordenado:
                print("  Aviso: Não foi possível aplicar ordenação. Paginação pode ser inconsistente.")
            
            # Aplica paginação usando range: range(início, fim) onde fim é inclusivo
            # range(0, 999) busca registros 0-999 (1000 registros)
            # range(1000, 1999) busca registros 1000-1999 (próximos 1000)
            inicio = offset
            fim = offset + page_size - 1
            print(f"  Buscando registros {inicio} a {fim}...")
            
            resp = query.range(inicio, fim).execute()
            
            # Verifica se a resposta contém dados válidos
            if not hasattr(resp, 'data') or resp.data is None:
                if offset == 0:
                    print("  Aviso: Resposta sem dados")
                    return pd.DataFrame()
                break
            
            # Verifica se não é uma resposta de erro (OpenAPI/Swagger)
            if isinstance(resp.data, dict) and 'swagger' in resp.data:
                if offset == 0:
                    raise Exception(f"Tabela '{SUPABASE_TABELA_LOG}' não encontrada. Verifique o nome da tabela no Supabase.")
                break
            
            # Se não há dados nesta página, terminamos
            if not resp.data or len(resp.data) == 0:
                print(f"  Nenhum dado retornado para offset {offset}. Finalizando.")
                break
            
            # Verifica duplicatas (para debug)
            novos_registros = 0
            for registro in resp.data:
                # Usa CHAVE_NF como identificador único, ou cria um hash do registro
                chave_id = registro.get("CHAVE_NF") or str(registro)
                if chave_id not in seen_keys:
                    seen_keys.add(chave_id)
                    novos_registros += 1
            
            if novos_registros < len(resp.data):
                print(f"  Aviso: Detectados {len(resp.data) - novos_registros} registros duplicados nesta página.")
            
            # Adiciona os dados desta página
            all_data.extend(resp.data)
            total_loaded += len(resp.data)
            print(f"  Carregados {total_loaded} registros (página com {len(resp.data)} registros)")
            
            # Se retornou menos que o page_size, chegamos ao fim
            if len(resp.data) < page_size:
                print(f"  Última página completa. Total: {total_loaded} registros.")
                break
            
            # Prepara para próxima página
            offset += page_size
            
            # Proteção contra loop infinito (máximo de 1 milhão de registros)
            if offset > 1000000:
                print(f"\n  Aviso: Limite de segurança atingido (1M registros)")
                break
        
        print(f"\n  Total de {total_loaded} registros carregados ({len(seen_keys)} únicos).")
        
        if not all_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(all_data)
        return df
    except Exception as e:
        error_msg = str(e)
        if 'swagger' in error_msg.lower() or 'openapi' in error_msg.lower():
            raise Exception(
                f"Erro: Tabela '{SUPABASE_TABELA_LOG}' não encontrada no Supabase.\n"
                f"Verifique se o nome da tabela está correto no secret SUPABASE_TABELA_LOG.\n"
                f"Erro original: {error_msg[:200]}"
            )
        raise


def enriquecer_dados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquece o DataFrame com informações extraídas das chaves NF.
    Adiciona colunas: NF, CNPJ, CNPJ_Formatado, Volume_Atual, Total_Volumes, Base_NF
    """
    if df.empty:
        return df
    
    dados_enriquecidos = []
    
    for _, row in df.iterrows():
        chave = row.get("CHAVE_NF", "")
        
        # Verifica se é etiqueta única (27 caracteres) ou NF (48 caracteres)
        if len(chave) == 27:
            # Etiqueta única - não tem NF/CNPJ
            nf = None
            cnpj = None
            info_vol = None
        elif len(chave) == 48:
            # NF - extrai informações
            nf, cnpj = extrair_nf_cnpj(chave)
            info_vol = extrair_info_volume(chave)
        else:
            # Formato desconhecido
            nf = None
            cnpj = None
            info_vol = None
        
        dados_enriquecidos.append({
            "OF": row.get("OF"),
            "CHAVE_NF": chave,
            "NF": nf if nf else "N/A",
            "CNPJ": cnpj if cnpj else "N/A",
            "CNPJ_Formatado": formatar_cnpj(cnpj) if cnpj else "N/A",
            "Volume_Atual": info_vol[1] if info_vol else None,
            "Total_Volumes": info_vol[2] if info_vol else None,
            "Base_NF": info_vol[0] if info_vol else None,
            "Bipado_em": row.get("bipado_em"),
            "Removido_em": row.get("removido_em"),
            "Inclusao_Manual": bool(row.get("INCLUSAO_MANUAL", False)),
        })
    
    return pd.DataFrame(dados_enriquecidos)


def main():
    """Função principal."""
    print("=" * 60)
    print("Sincronização Supabase -> CSV")
    print("=" * 60)
    
    # Valida variáveis de ambiente
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERRO: SUPABASE_URL e SUPABASE_KEY devem estar configurados!")
        exit(1)
    
    # Conecta ao Supabase
    try:
        print("\nConectando ao Supabase...")
        client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✓ Conexão estabelecida com sucesso!")
    except Exception as e:
        print(f"✗ Erro ao conectar ao Supabase: {e}")
        exit(1)
    
    # Carrega todos os dados (todas as OFs, incluindo removidos)
    print("\nCarregando todos os dados do Supabase...")
    print(f"  Tabela configurada: {SUPABASE_TABELA_LOG}")
    try:
        df = carregar_dados_supabase(client, incluir_removidas=True)
        
        if df.empty:
            print("Nenhum dado encontrado no Supabase.")
            # Cria CSV vazio para manter o arquivo no repositório
            pd.DataFrame().to_csv(CSV_FILE, index=False)
            print(f"CSV vazio criado: {CSV_FILE}")
            return
        
        print(f"✓ {len(df)} registro(s) carregado(s).")
        print(f"  Colunas encontradas: {', '.join(df.columns)}")
    except Exception as e:
        print(f"✗ Erro ao carregar dados: {e}")
        print("\nDicas para resolver:")
        print("  1. Verifique se o nome da tabela está correto no secret SUPABASE_TABELA_LOG")
        print("  2. Confirme que a tabela existe no seu projeto Supabase")
        print("  3. Verifique se a chave de API tem permissão para ler a tabela")
        exit(1)
    
    # Enriquece os dados
    print("\nEnriquecendo dados...")
    df_enriquecido = enriquecer_dados(df)
    print("✓ Dados enriquecidos com sucesso!")
    
    # Salva como CSV
    print(f"\nSalvando CSV: {CSV_FILE}")
    try:
        df_enriquecido.to_csv(CSV_FILE, index=False, encoding='utf-8-sig')
        print(f"✓ CSV salvo com sucesso!")
        print(f"  Total de registros: {len(df_enriquecido)}")
        print(f"  Colunas: {', '.join(df_enriquecido.columns)}")
    except Exception as e:
        print(f"✗ Erro ao salvar CSV: {e}")
        exit(1)
    
    print("\n" + "=" * 60)
    print("Processo concluído!")
    print("=" * 60)


if __name__ == "__main__":
    main()
