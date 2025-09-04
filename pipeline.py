
import papermill as pm
import logging
from datetime import datetime

# Configura o logger
logging.basicConfig(
    filename='pipeline.log',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)

def executar_etapa(nome_entrada, nome_saida):
    try:
        logging.info(f"Iniciando etapa: {nome_entrada}")
        pm.execute_notebook(nome_entrada, nome_saida)
        logging.info(f"Etapa concluída com sucesso: {nome_entrada}")
    except Exception as e:
        logging.error(f"Erro na etapa {nome_entrada}: {e}")
        raise

def main():
    logging.info("### Início da execução da pipeline ###")

    executar_etapa("ingestao.ipynb", "ingestao_output.ipynb")
    executar_etapa("transformacao.ipynb", "transformacao_output.ipynb")
    executar_etapa("consumo.ipynb", "consumo_output.ipynb")

    logging.info("### Fim da execução da pipeline ###")

if __name__ == "__main__":
    main()
