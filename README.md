# pipeline-etl
Pipeline ETL com integração OpenAI e Santander Dev Week API
import requests
import json
import pandas as pd
import openai

# Configuração das APIs
openai.api_key = "SUA_CHAVE_API_OPENAI"
API_URL = "https://sdw-2023-prd.up.railway.app/users/"

# Funções ETL (cole aqui todo o código do pipeline)
def extrair_dados_planilha(arquivo):
    df = pd.read_csv(arquivo)
    return df['id'].tolist()

def extrair_dados_api(ids_usuarios):
    usuarios = []
    for id_usuario in ids_usuarios:
        response = requests.get(f"{API_URL}{id_usuario}")
        if response.status_code == 200:
            usuarios.append(response.json())
    return usuarios

def gerar_mensagem_marketing(usuario):
    prompt = f"""
    Crie uma mensagem de marketing personalizada para:
    Nome: {usuario['name']}
    Conta: {usuario.get('account', 'N/A')}
    
    A mensagem deve ser amigável sobre produtos financeiros.
    """
    
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=100
    )
    return response.choices[0].message.content

def transformar_dados(usuarios):
    for usuario in usuarios:
        usuario['mensagem_marketing'] = gerar_mensagem_marketing(usuario)
    return usuarios

def carregar_dados_api(usuarios_transformados):
    for usuario in usuarios_transformados:
        payload = {"mensagem": usuario['mensagem_marketing']}
        response = requests.post(f"{API_URL}{usuario['id']}/messages", json=payload)
        
        if response.status_code == 200:
            print(f"✅ Mensagem enviada para {usuario['name']}")
        else:
            print(f"❌ Erro ao enviar para {usuario['name']}")

def executar_pipeline(arquivo_planilha):
    print("📊 Extraindo IDs...")
    ids = extrair_dados_planilha(arquivo_planilha)
    
    print("🌐 Extraindo dados da API...")
    usuarios = extrair_dados_api(ids)
    
    print("🤖 Transformando com IA...")
    usuarios_transformados = transformar_dados(usuarios)
    
    print("📤 Carregando na API...")
    carregar_dados_api(usuarios_transformados)
    
    print("✅ Pipeline concluído!")

if __name__ == "__main__":
    executar_pipeline("usuarios.csv")
