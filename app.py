from flask import Flask, request, jsonify
import psycopg2
from dotenv import load_dotenv
from datetime import datetime
import os
import schedule
import threading
import time
import requests
import json

app = Flask(__name__)
load_dotenv()

# Configurações do PostgreSQL
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')


dominio_url_map = {
    "sugoihomolog": "https://qa-back.db2tech.com.br/api/",
    # Adicione outros domínios e URLs conforme necessário
}

def connect():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    return conn

@app.route('/baixa_titulo', methods=['POST'])
def baixa():
    try:
        data = request.json
        
        # Verifica se os campos necessários estão presentes no JSON de entrada
        parametros = data.get('parametros', {})
        titulo = parametros.get('titulo')
        empresa = parametros.get('empresa')
        conta = parametros.get('conta')
        parcela = parametros.get('parcela')

        if not (titulo and empresa and conta and parcela):
            return jsonify({"error": "Campos obrigatórios ausentes"}), 400

        # Configura o status para 'pendente' se não estiver presente nos parâmetros
        status = data.get('status', 'pendente')
        
        # Adiciona a data atual ao campo 'data_solicitacao'
        data_solicitacao = datetime.now().isoformat()
        
        # Insere os dados no banco de dados
        conn = connect()
        cur = conn.cursor()
        cur.execute("INSERT INTO Baixa (titulo, dominio, empresa, conta, parcela, data_solicitacao, status) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (titulo, data.get('env'), empresa, conta, parcela, data_solicitacao, status))
        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"message": "Dados de baixa armazenados com sucesso!"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/movimentacao_bancaria', methods=['POST'])
def movimentacao_bancaria():
    try:
        data = request.json
        
        # Verifica se os campos necessários estão presentes no JSON de entrada
        parametros = data.get('parametros', {})
        titulo = parametros.get('titulo')
        empresa = parametros.get('empresa')
        conta = parametros.get('conta')
        operacao = parametros.get('operacao')
        documento = parametros.get('documento')
        numero_documento = parametros.get('numero_documento')
        parcela = parametros.get('parcela')
        valor = parametros.get('valor')
        centro_custo = parametros.get('centro_custo')
        plano_financeiro = parametros.get('plano_financeiro')

        if not (titulo and empresa and conta and operacao and documento and numero_documento and parcela and valor and centro_custo and plano_financeiro):
            return jsonify({"error": "Campos obrigatórios ausentes"}), 400
        
        # Configura o status para 'pendente'
        status = data.get('status', 'pendente')
        
        # Adiciona a data atual ao campo 'data_processamento'
        data_processamento = datetime.now().isoformat()
        
        # Insere os dados no banco de dados
        conn = connect()
        cur = conn.cursor()
        cur.execute("INSERT INTO MovimentacaoBancaria (titulo, dominio, status, empresa, conta, operacao, documento, numero_documento, parcela, valor, centro_custo, plano_financeiro, data_processamento) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (titulo, data.get('env'), status, empresa, conta, operacao, documento, numero_documento, parcela, valor, centro_custo, plano_financeiro, data_processamento))
        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"message": "Dados de movimentação bancária armazenados com sucesso!"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def obter_endpoint(dominio):
    # Verifica se o domínio está mapeado
    if dominio in dominio_url_map:
        return dominio_url_map[dominio] + "Fidc/RecevedStatusRPA"
    else:
        # Caso o domínio não esteja mapeado, retorne um valor padrão ou trate conforme necessário
        return None

def verificar_registros_concluidos():
    try:
        # Verifica os títulos concluídos na tabela Baixa
        conn = connect()
        cur = conn.cursor()
        print("----")
        print("VENDO TITULO DE BAIXA")
        cur.execute("SELECT * FROM Baixa WHERE status = 'concluido'")
        titulos_concluidos_baixa = cur.fetchall()
        cur.close()

        if titulos_concluidos_baixa:
            for titulo in titulos_concluidos_baixa:
                # Monta o JSON com os dados relevantes
                dominio = titulo[2]
                empresa = titulo[3]
                conta = titulo[4]
                titulo_id = titulo[1]
                parcela = titulo[5]
                status = "concluido"
                output = titulo[8]  # Substituir pelo valor adequado

                payload = {
                    "env": dominio,
                    "template": "aio-bot-put-sienge-baixa-titulos",
                    "started": datetime.utcnow().isoformat(),
                    "status": status,
                    "credential": "string",
                    "parametros": {
                        "empresa": empresa,
                        "conta": conta,
                        "titulo": titulo_id,
                        "parcela": parcela
                    },
                    "key": "string",
                    "output": output
                }
                print(payload)
                
                # Obtem o endpoint com base no domínio
                endpoint_baixa = obter_endpoint(dominio)
                if endpoint_baixa:
                    #response_baixa = requests.post(endpoint_baixa, json=payload)
                    #print("Response Baixa:", response_baixa.text)
                    print(endpoint_baixa)
                else:
                    print("Domínio não mapeado:", dominio)

                # Envia o JSON para o endpoint de baixa
                # endpoint_baixa = "https://qa-back.db2tech.com.br/api/Fidc/RecevedStatusRPA"
                # response_baixa = requests.post(endpoint_baixa, json=payload)
                # print("Response Baixa:", response_baixa.text)



        # Verifica os títulos concluídos na tabela MovimentacaoBancaria
        # cur = conn.cursor()
        # cur.execute("SELECT * FROM MovimentacaoBancaria WHERE status = 'Concluído'")
        # titulos_concluidos_movimentacao = cur.fetchall()
        # cur.close()

        # if titulos_concluidos_movimentacao:
        #     for titulo in titulos_concluidos_movimentacao:
        #         # Monta o JSON com os dados relevantes
        #         dominio = titulo[1]
        #         empresa = titulo[3]
        #         conta = titulo[4]
        #         titulo_id = titulo[2]
        #         operacao = titulo[5]
        #         documento = titulo[6]
        #         numero_documento = titulo[7]
        #         parcela = titulo[8]
        #         valor = titulo[9]
        #         centro_custo = titulo[10]
        #         plano_financeiro = titulo[11]
        #         status = "concluido"
        #         output = "output_aqui"  # Substituir pelo valor adequado

        #         payload = {
        #             "env": dominio,
        #             "template": "aio-bot-put-sienge-movimentacao-bancaria",
        #             "started": datetime.utcnow().isoformat(),
        #             "finished": datetime.utcnow().isoformat(),
        #             "credential": "string",
        #             "status": status,
        #             "key": "string",
        #             "output": output,
        #             "parametros": {
        #                 "empresa": empresa,
        #                 "conta": conta,
        #                 "titulo": titulo_id,
        #                 "operacao": operacao,
        #                 "documento": documento,
        #                 "numero_documento": numero_documento,
        #                 "parcela": parcela,
        #                 "valor": valor,
        #                 "centro_custo": centro_custo,
        #                 "plano_financeiro": plano_financeiro
        #             }
        #         }

        #         # Envia o JSON para o endpoint de movimentação bancária
        #         endpoint_movimentacao = "https://qa-back.db2tech.com.br/api/Fidc/RecevedStatusRPABank"
        #         response_movimentacao = requests.post(endpoint_movimentacao, json=payload)
        #         print("Response Movimentação Bancária:", response_movimentacao.text)

    except Exception as e:
        print("Erro ao verificar registros concluídos:", e)

# Agende a verificação a cada 5 minutos
schedule.every(5).minutes.do(verificar_registros_concluidos)

# Função para iniciar a verificação em uma thread separada
def iniciar_verificacao_conclusao():
    while True:
        schedule.run_pending()
        time.sleep(1)

# Inicia a verificação em uma thread separada
thread = threading.Thread(target=iniciar_verificacao_conclusao)
thread.daemon = True  # Define a thread como um daemon para que ela termine quando o programa principal terminar
thread.start()

if __name__ == '__main__':
    app.run(debug=True)
