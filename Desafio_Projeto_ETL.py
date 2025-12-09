import pandas as pd
from google import genai
from google.genai.errors import APIError
import os


# --- Configurações ---
ARQUIVO_CLIENTES = 'Informe aqui o caminho do arquivo '
MODELO_GEMINI = 'gemini-2.5-flash' 
# A forma recomendada é definir a variável de ambiente: os.environ['GEMINI_API_KEY'] = 'SUA_CHAVE_API_GEMINI'
# Para este exemplo simplificado, usaremos o getenv
API_KEY = os.getenv("GEMINI_API_KEY", "informe aqui a chave")

# Inicializa o cliente Gemini
try:
    client = genai.Client(api_key=API_KEY)
except Exception as e:
    print(f"Erro ao inicializar o cliente Gemini: {e}")
    client = None

# ----------------------------------------------------
## 👟 FASE 1: EXTRAÇÃO (Extraction - E)
# ----------------------------------------------------
def extrair_dados(CAMINHO):
    """
    Carrega o arquivo CSV de clientes.
    """
    print(f"## 1. Extração: Carregando dados de '{ARQUIVO_CLIENTES}'...")
    try:
        # Lê o CSV para um DataFrame do pandas
        df_clientes = pd.read_csv(ARQUIVO_CLIENTES)
        print(f"   -> {len(df_clientes)} clientes carregados com sucesso.")
        return df_clientes
    except FileNotFoundError:
        print(f"   -> ERRO: Arquivo '{ARQUIVO_CLIENTES}' não encontrado.")
        return None
    except pd.errors.EmptyDataError:
        print(f"   -> ERRO: Arquivo '{ARQUIVO_CLIENTES}' está vazio.")
        return None
    except Exception as e:
        print(f"   -> ERRO inesperado na leitura do arquivo: {e}")
        return None  
    
    # ----------------------------------------------------
## 💬 FASE 2: TRANSFORMAÇÃO (Transformation - T)
# ----------------------------------------------------
def gerar_frase_marketing(nome_cliente):
    """
    Gera uma frase de marketing personalizada usando a API Gemini.
    """
    if not client:
        return "Cliente Gemini não inicializado. Não foi possível gerar a frase."
        
    print(f"   -> Gerando frase de marketing para {nome_cliente}...")

    # Prompt para a IA
    prompt = (
        f"Gere uma frase de marketing de promoção de calçados, amigável e concisa, "
        f"especialmente direcionada para o cliente '{nome_cliente}', incentivando-o a "
        f"voltar a comprar na nossa loja de calçados. Use um tom de convite e crie uma oferta."
    )
    
    try:
        # Chamada da API Gemini
        response = client.models.generate_content(
            model=MODELO_GEMINI,
            contents=[prompt]
        )
        # Limpa o texto de possíveis quebras de linha ou espaços extras
        frase = response.text.strip()
        return frase
    except APIError as e:
        # Captura erros específicos da API (ex: chave inválida, limite excedido)
        print(f"      - ERRO API para {nome_cliente}: {e}")
        return f"[ERRO DE API] Não foi possível gerar a frase para {nome_cliente}."
    except Exception as e:
        # Captura outros erros
        print(f"      - ERRO inesperado para {nome_cliente}: {e}")
        return f"[ERRO GERAL] Não foi possível gerar a frase para {nome_cliente}."


def transformar_dados(df_clientes):
    """
    Aplica a transformação (geração da frase de marketing) a cada cliente.
    """
    if df_clientes is None:
        return None
        
    print("\n## 2. Transformação: Gerando mensagens de marketing com Gemini...")
    
    # Cria uma nova coluna aplicando a função de geração de frases a cada linha do DataFrame
    df_clientes['Mensagem_Marketing'] = df_clientes['Nome'].apply(gerar_frase_marketing)
    
    print("   -> Geração de mensagens concluída.")
    return df_clientes

# ----------------------------------------------------
## 📤 FASE 3: CARREGAMENTO (Loading - L)
# ----------------------------------------------------
def carregar_dados(df_transformado):
    """
    Apresenta o nome do cliente e a mensagem de marketing no terminal.
    """
    if df_transformado is None:
        return
        
    print("\n## 3. Carregamento: Exibindo resultados no terminal...")
    
    # Itera sobre as linhas do DataFrame transformado
    for index, row in df_transformado.iterrows():
        nome = row['Nome']
        mensagem = row['Mensagem_Marketing']
        
        print("-" * 40)
        print(f"**Cliente:** {nome}")
        print(f"**Mensagem:** {mensagem}")
    print("-" * 40)
    print("   -> Carregamento finalizado.")


# ----------------------------------------------------
## 🚀 FUNÇÃO PRINCIPAL
# ----------------------------------------------------
def rodar_etl_calcados():
    """
    Orquestra as fases de Extração, Transformação e Carregamento.
    """
    print("--- Início do Processo ETL para Loja de Calçados ---")
    
    # 1. Extração
    df_extraido = extrair_dados(ARQUIVO_CLIENTES)
    
    if df_extraido is None:
        print("\nProcesso ETL interrompido devido a falha na Extração.")
        return
        
    # 2. Transformação
    df_transformado = transformar_dados(df_extraido)
    
    if df_transformado is None:
        print("\nProcesso ETL interrompido devido a falha na Transformação.")
        return
        
    # 3. Carregamento
    carregar_dados(df_transformado)
    
    print("\n--- Fim do Processo ETL ---")

# Executa o programa
if __name__ == "__main__":
    rodar_etl_calcados()
