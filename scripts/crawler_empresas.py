"""
Crawler unificado para extração e processamento de dados das empresas:
IMBEL, CEITEC e TELEBRAS.

Este script permite o crawling incremental (apenas novas páginas) e
extração de documentos (PDF, DOC, ZIP, etc.) com suporte à extração
de conteúdo de arquivos compactados.
"""

import asyncio
import os
import re
import argparse
import psycopg2
import aiohttp
import mimetypes
import urllib.parse
import zipfile
import uuid
import logging
import shutil
from urllib.parse import urlparse
from psycopg2.extensions import register_adapter, AsIs
from dotenv import load_dotenv
from typing import List, Dict, Set, Optional, Any, Tuple
from crawl4ai import AsyncWebCrawler, CacheMode
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('crawler.log')
    ]
)
logger = logging.getLogger('crawler_empresas')

# =============================================================================
# CONFIGURAÇÕES
# =============================================================================

# Configuração das empresas
EMPRESAS = {
    "imbel": {
        "url": "https://www.imbel.gov.br/",
        "tabela": "tbl_paginas_imbel",
        "max_depth": 10,
        "include_external": False,
        "excluded_tags": ['form', 'header', 'footer', 'nav', 'img-logo-imbel', 'barra-brasil'],
        "excluded_selector": ".blog-anteriores,.cookies-eu-banner,.header-logo-t,.conteudo-barra-brasil"
    },
    "ceitec": {
        # O URL sem protocolo funcionou melhor no script antigo
        "url": "http://www.ceitec-sa.com/",
        "tabela": "tbl_paginas_ceitec",
        "max_depth": 10,
        "include_external": True,
        # Vamos remover as exclusões para permitir rastrear todo o conteúdo
        "excluded_tags": [],
        "excluded_selector": "",
        "ignore_ssl_errors": True,
        # Adicionar flag especial para usar configurações específicas
        "use_simplified_crawler": True
    },
    "telebras": {
        "url": "https://www.telebras.com.br/",
        "tabela": "tbl_paginas_telebras",
        "max_depth": 10,
        "include_external": False,
        "excluded_tags": ['form', 'header', 'footer', 'nav', 'barra-brasil'],
        "excluded_selector": ".blog-anteriores,.cookies-eu-banner,.header-logo-t,.conteudo-barra-brasil"
    }
}

# Extensões e padrões de URL para arquivos que serão baixados
DOCUMENT_EXTENSIONS = [
    '.pdf', '.docx', '.doc', '.xlsx', '.xls', '.pptx', '.ppt',
    '.csv', '.zip', '.rar', '.7z', '.gz', '.tar', '.bz2', '.xz',
    '.odt', '.ods', '.odp', '.txt', '.rtf', '.epub', '.mobi',
    '.pub', '.vsd', '.msg', '.xml', '.json', '.jpg', '.jpeg',
    '.png', '.gif', '.mp3', '.mp4', '.wav'
]

# Extensões de arquivos que podem ser extraídos
EXTRACTABLE_EXTENSIONS = ['.zip']

# Padrões de caminhos que indicam arquivos de documentos (para URLs sem extensão)
DOCUMENT_PATH_PATTERNS = [
    '/storage/', '/download/', '/arquivos/', '/documentos/',
    '/files/', '/attachments/', '/anexos/'
]

# Adicionar constante para controle de cache
CACHE_DIR = os.path.join(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))), 'cache')

# Adicionar constante para diretório de saída
OUTPUT_DIR = os.path.join(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))), 'output')

# Adicionar funções para gerenciar cache


def get_cache_filename(empresa: str) -> str:
    """
    Retorna o nome do arquivo de cache para uma empresa.

    Args:
        empresa: Nome da empresa

    Returns:
        Caminho do arquivo de cache
    """
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"{empresa}_crawled_urls.txt")


def load_crawled_urls_from_cache(empresa: str) -> Set[str]:
    """
    Carrega URLs já processadas do cache.

    Args:
        empresa: Nome da empresa

    Returns:
        Conjunto de URLs já processadas
    """
    cache_file = get_cache_filename(empresa)
    if os.path.exists(cache_file):
        with open(cache_file, 'r') as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def save_crawled_urls_to_cache(empresa: str, urls: Set[str]) -> None:
    """
    Salva URLs processadas no cache.

    Args:
        empresa: Nome da empresa
        urls: Conjunto de URLs processadas
    """
    cache_file = get_cache_filename(empresa)
    with open(cache_file, 'w') as f:
        for url in urls:
            f.write(f"{url}\n")


def get_document_cache_filename(empresa: str) -> str:
    """
    Retorna o nome do arquivo de cache para documentos de uma empresa.

    Args:
        empresa: Nome da empresa

    Returns:
        Caminho do arquivo de cache de documentos
    """
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"{empresa}_document_urls.txt")


def load_documents_from_cache(empresa: str) -> Set[str]:
    """
    Carrega URLs de documentos do cache.

    Args:
        empresa: Nome da empresa

    Returns:
        Conjunto de URLs de documentos
    """
    cache_file = get_document_cache_filename(empresa)
    if os.path.exists(cache_file):
        with open(cache_file, 'r') as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def save_documents_to_cache(empresa: str, urls: List[str]) -> None:
    """
    Salva URLs de documentos no cache.

    Args:
        empresa: Nome da empresa
        urls: Lista de URLs de documentos
    """
    cache_file = get_document_cache_filename(empresa)
    with open(cache_file, 'w') as f:
        for url in urls:
            f.write(f"{url}\n")


def get_output_dir(empresa: str) -> str:
    """
    Retorna o diretório de saída para os arquivos da empresa.

    Args:
        empresa: Nome da empresa

    Returns:
        Caminho do diretório de saída
    """
    output_dir = os.path.join(OUTPUT_DIR, empresa)
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def get_html_output_dir(empresa: str) -> str:
    """
    Retorna o diretório de saída para arquivos HTML/markdown da empresa.

    Args:
        empresa: Nome da empresa

    Returns:
        Caminho do diretório de saída para HTML/markdown
    """
    html_dir = os.path.join(get_output_dir(empresa), 'html')
    os.makedirs(html_dir, exist_ok=True)
    return html_dir


def get_documents_output_dir(empresa: str) -> str:
    """
    Retorna o diretório de saída para documentos da empresa.

    Args:
        empresa: Nome da empresa

    Returns:
        Caminho do diretório de saída para documentos
    """
    docs_dir = os.path.join(get_output_dir(empresa), 'documentos')
    os.makedirs(docs_dir, exist_ok=True)
    return docs_dir


def get_extracted_output_dir(empresa: str) -> str:
    """
    Retorna o diretório de saída para arquivos extraídos de ZIPs da empresa.

    Args:
        empresa: Nome da empresa

    Returns:
        Caminho do diretório de saída para arquivos extraídos
    """
    extracted_dir = os.path.join(get_output_dir(empresa), 'extraidos')
    os.makedirs(extracted_dir, exist_ok=True)
    return extracted_dir


# Adicionar constantes para controle de tentativas e timeouts
MAX_RETRIES = 3  # Número máximo de tentativas para baixar um documento
DOWNLOAD_TIMEOUT = 60  # Timeout em segundos para download de documentos
CONNECT_TIMEOUT = 30  # Timeout para conexão inicial
GLOBAL_TIMEOUT = 3600  # Timeout global para a sessão completa (1 hora)

# =============================================================================
# FUNÇÕES DE UTILIDADE
# =============================================================================


def sanitize_filename(url: str) -> str:
    """
    Converte uma URL em um nome de arquivo seguro.

    Args:
        url: URL a ser convertida em nome de arquivo

    Returns:
        Nome de arquivo seguro
    """
    # Remover protocolo e www
    name = re.sub(r'https?://(www\.)?', '', url)
    # Substituir caracteres não permitidos em nomes de arquivo
    name = re.sub(r'[\\/*?:"<>|]', '_', name)
    # Substituir barras e pontos por underscores
    name = name.replace('/', '_').replace('.', '_')
    # Limitar o tamanho do nome do arquivo
    if len(name) > 100:
        name = name[:100]
    return name


def get_db_connection():
    """
    Estabelece conexão com o banco de dados usando variáveis de ambiente.

    Returns:
        Conexão com o banco de dados PostgreSQL

    Raises:
        EnvironmentError: Se variáveis de ambiente necessárias não forem encontradas
    """
    load_dotenv()  # Carrega as variáveis do .env

    # Verificar se as variáveis de ambiente necessárias estão presentes
    required_vars = ['DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_HOST']
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        raise EnvironmentError(
            f"Variáveis de ambiente obrigatórias não encontradas: {', '.join(missing_vars)}. "
            f"Certifique-se de criar um arquivo .env baseado no .env.example."
        )

    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST')
    )


def extract_subdomain(url: str) -> str:
    """
    Extrai o subdomínio de uma URL.

    Args:
        url: URL da qual extrair o subdomínio

    Returns:
        Subdomínio ou string vazia se não encontrado
    """
    # Remove protocolo se existir
    url = re.sub(r'https?://', '', url)
    # Divide por pontos e pega todos menos o último e penúltimo (domínio principal)
    parts = url.split('.')
    if len(parts) > 2:
        return parts[0]
    return ''


def extract_path_tags(url: str) -> List[str]:
    """
    Extrai tags do caminho da URL.

    Args:
        url: URL da qual extrair tags

    Returns:
        Lista de tags extraídas do caminho
    """
    # Remove protocolo e domínio
    path = re.sub(r'https?://[^/]+/', '', url)
    # Remove parâmetros de query e âncoras
    path = re.sub(r'[?#].*$', '', path)
    # Divide o caminho em partes
    parts = [p for p in path.split('/') if p]

    # Lista para armazenar as tags processadas
    processed_tags = []

    # Processa cada parte do caminho
    for part in parts:
        # Se for numérico ou contiver números, pega a parte anterior se existir
        if re.search(r'\d', part):
            # Procura a última tag não numérica adicionada
            previous_parts = [
                p for p in processed_tags if not re.search(r'\d', p)]
            if previous_parts:
                continue  # Já temos a tag categoria (ex: 'noticias')
            else:
                # Se não houver tag anterior, tenta extrair a parte não numérica
                non_numeric = re.sub(r'\d+', '', part).strip(',.-_')
                if non_numeric:
                    processed_tags.append(non_numeric)
        else:
            processed_tags.append(part)

    return processed_tags


def adapt_list(lst):
    """
    Adapta uma lista Python para array PostgreSQL.

    Args:
        lst: Lista a ser adaptada

    Returns:
        Representação SQL do array
    """
    if lst is None:
        return AsIs('NULL')
    return AsIs("'{%s}'" % ','.join(str(x) for x in lst))


def get_existing_urls(tabela: str) -> Set[str]:
    """
    Obtém todas as URLs já processadas para uma determinada empresa.

    Args:
        tabela: Nome da tabela no banco de dados

    Returns:
        Conjunto de URLs já processadas
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT link FROM {tabela}")
        urls = {row[0] for row in cur.fetchall()}
        return urls
    except Exception as e:
        logger.error(f"Erro ao obter URLs existentes: {e}")
        return set()
    finally:
        cur.close()
        conn.close()


def is_document_url(url: str) -> bool:
    """
    Verifica se uma URL aponta para um documento que deve ser baixado.
    Checa tanto a extensão quanto padrões de caminho comuns para arquivos.

    Args:
        url: URL a ser verificada

    Returns:
        True se a URL for de um documento, False caso contrário
    """
    # Verificar se a URL está vazia ou inválida
    if not url or not isinstance(url, str):
        return False

    try:
        if not url.startswith(('http://', 'https://')):
            return False

        # Parsear a URL
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()

        # Verificar extensões conhecidas de documentos - esta é a parte mais importante
        if any(path.endswith(ext) for ext in DOCUMENT_EXTENSIONS):
            logger.info(f"Identificado documento por extensão: {url}")
            return True

        # Verificar padrões de caminho comuns para arquivos
        if any(pattern in path for pattern in DOCUMENT_PATH_PATTERNS):
            # Se o caminho contém um padrão de documento, verificar se não termina com extensões de página web
            if not path.endswith(('.html', '.htm', '.php', '.asp', '.aspx', '.jsp')):
                logger.info(
                    f"Identificado documento por padrão de caminho: {url}")
                return True

        # Verificar padrões específicos em parâmetros de query que podem indicar download
        query = parsed_url.query.lower()
        if ('download=' in query or 'file=' in query or 'attachment=' in query or
                'document=' in query or 'arquivo=' in query):
            logger.info(
                f"Identificado documento por parâmetro de query: {url}")
            return True

        # Verificar keywords específicas no caminho
        document_keywords = ['download', 'document', 'file',
                             'arquivo', 'edital', 'formulario', 'anexo']
        if any(keyword in path.lower() for keyword in document_keywords):
            logger.info(
                f"Identificado documento por keyword no caminho: {url}")
            return True

        return False
    except Exception as e:
        logger.error(f"Erro ao verificar URL de documento {url}: {str(e)}")
        return False


def is_definitely_document_url(url: str) -> bool:
    """
    Verificação rigorosa para determinar se uma URL é definitivamente um documento.
    Esta função será usada para evitar que o crawler tente navegar para arquivos de documentos.

    Args:
        url: URL a ser verificada

    Returns:
        True se a URL é definitivamente um documento, False se não temos certeza
    """
    if not url or not isinstance(url, str):
        return False

    try:
        # Verificar extensões de documento na URL - esta é a verificação mais segura
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()

        # Verificar extensões de documentos
        if any(path.endswith(ext) for ext in DOCUMENT_EXTENSIONS):
            return True

        # Verificar padrões óbvios no caminho que indicam documentos
        doc_patterns = ['/download/', '/files/', '/docs/', '/documents/', '/documentos/',
                        '/arquivos/', '/anexos/', '/storage/']
        if any(pattern in path for pattern in doc_patterns):
            for ext in DOCUMENT_EXTENSIONS:
                if ext[1:] in path:  # ext[1:] remove o ponto inicial
                    return True

        # Verificar padrões em parâmetros de query
        query = parsed_url.query.lower()
        doc_queries = ['download=', 'file=', 'doc=',
                       'document=', 'attachment=', 'filename=']
        if any(q in query for q in doc_queries):
            return True

        return False
    except Exception as e:
        logger.debug(f"Erro ao verificar URL de documento {url}: {str(e)}")
        return False


def get_file_type(file_path: str) -> str:
    """
    Determina o tipo de arquivo com base na extensão.

    Args:
        file_path: Caminho do arquivo

    Returns:
        Tipo de arquivo como string
    """
    _, ext = os.path.splitext(file_path.lower())

    if ext == '.pdf':
        return 'pdf'
    elif ext in ['.docx', '.doc']:
        return 'word'
    elif ext in ['.xlsx', '.xls']:
        return 'excel'
    elif ext in ['.pptx', '.ppt']:
        return 'powerpoint'
    elif ext == '.csv':
        return 'csv'
    elif ext in ['.zip', '.rar', '.7z', '.gz', '.tar', '.bz2', '.xz']:
        return 'arquivo_compactado'
    elif ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg']:
        return 'imagem'
    elif ext in ['.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm']:
        return 'video'
    elif ext in ['.mp3', '.wav', '.ogg', '.flac', '.aac']:
        return 'audio'
    elif ext == '.txt':
        return 'texto'
    elif ext == '.html' or ext == '.htm':
        return 'html'
    else:
        return 'documento'


def guess_file_extension(content_type: str, url: str) -> str:
    """
    Tenta adivinhar a extensão do arquivo com base no Content-Type e URL.

    Args:
        content_type: Tipo MIME do conteúdo
        url: URL do arquivo

    Returns:
        Extensão do arquivo
    """
    # Tentar obter da URL primeiro
    _, ext = os.path.splitext(urlparse(url).path)
    if ext and len(ext) < 10:  # Se tiver uma extensão válida na URL
        return ext

    # Extensões comuns para certos tipos MIME
    mime_map = {
        'application/pdf': '.pdf',
        'application/msword': '.doc',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx',
        'application/vnd.ms-excel': '.xls',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': '.xlsx',
        'application/vnd.ms-powerpoint': '.ppt',
        'application/vnd.openxmlformats-officedocument.presentationml.presentation': '.pptx',
        'text/csv': '.csv',
        'application/zip': '.zip',
        'application/x-rar-compressed': '.rar',
        'application/x-7z-compressed': '.7z',
        'application/gzip': '.gz',
        'application/x-tar': '.tar',
        'application/x-bzip2': '.bz2',
        'application/x-xz': '.xz',
    }

    if content_type in mime_map:
        return mime_map[content_type]

    # Tentar mimetypes como fallback
    ext = mimetypes.guess_extension(content_type)
    if ext:
        return ext

    # Fallback genérico
    return '.bin'

# =============================================================================
# FUNÇÕES PARA PROCESSAR DOCUMENTOS
# =============================================================================


async def extract_links_from_page(page_content: str, base_url: str) -> List[str]:
    """
    Extrai links de HTML cru usando regex para processamento posterior.

    Args:
        page_content: Conteúdo HTML da página
        base_url: URL base para resolver links relativos

    Returns:
        Lista de links extraídos
    """
    links = []

    # Regex para extrair href de tags <a>
    href_pattern = re.compile(
        r'<a\s+(?:[^>]*?\s+)?href=(["\'])(.*?)\1', re.IGNORECASE)
    matches = href_pattern.findall(page_content)

    for _, href in matches:
        href = href.strip()
        if href and not href.startswith(('#', 'javascript:', 'mailto:')):
            # Resolve URLs relativas
            if not href.startswith(('http://', 'https://')):
                href = urllib.parse.urljoin(base_url, href)
            links.append(href)

    return links


def save_html_content(url: str, html_content: str, nome_empresa: str) -> str:
    """
    Salva o conteúdo HTML original em um arquivo.

    Args:
        url: URL da página
        html_content: Conteúdo HTML da página
        nome_empresa: Nome da empresa

    Returns:
        Caminho do arquivo salvo
    """
    html_dir = get_html_output_dir(nome_empresa)

    # Criar um nome de arquivo baseado na URL
    filename = sanitize_filename(url)

    # Garantir que a extensão seja .html
    if not filename.endswith('.html'):
        filename = f"{filename}.html"

    file_path = os.path.join(html_dir, filename)

    # Salvar o conteúdo no arquivo
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

    return file_path


async def download_document(url: str, nome_empresa: str, session: aiohttp.ClientSession) -> Tuple[bool, str, str]:
    """
    Baixa um documento e retorna seu caminho local, com sistema de retry.

    Args:
        url: URL do documento
        nome_empresa: Nome da empresa para determinar o diretório
        session: Sessão HTTP para fazer o download

    Returns:
        Tupla (sucesso, caminho_local, tipo_arquivo)
    """
    # Criar diretório para documentos na nova estrutura
    docs_dir = get_documents_output_dir(nome_empresa)

    # Determinar nome original do arquivo da URL
    parsed_url = urlparse(url)
    original_filename = os.path.basename(parsed_url.path)

    # Remover fragmentos (#) da URL que possam causar problemas
    clean_url = url.split('#')[0]

    # Se a URL termina com # ou tem apenas fragmentos, provavelmente não é um documento válido
    if clean_url.endswith('/') or not clean_url or clean_url == url.split('#')[0]:
        logger.warning(f"URL possivelmente inválida para documento: {url}")
        if '#' in url and not clean_url:
            logger.error(f"URL contém apenas um fragmento, ignorando: {url}")
            return False, "", ""

    # Limpar URLs duplicadas (problema observado com URLs como .pdf.pdf)
    if any(ext + ext in clean_url.lower() for ext in DOCUMENT_EXTENSIONS):
        for ext in DOCUMENT_EXTENSIONS:
            if ext + ext in clean_url.lower():
                clean_url = clean_url.replace(ext + ext, ext)

    # Se o nome original for válido, usá-lo; caso contrário, gerar um nome sanitizado
    if original_filename and len(original_filename) < 100 and not original_filename.endswith('/'):
        filename = original_filename
    else:
        # Gerar um nome de arquivo sanitizado
        filename = sanitize_filename(clean_url)
        # Tentar preservar a extensão
        _, original_extension = os.path.splitext(parsed_url.path)
        if original_extension and len(original_extension) < 10:
            filename = f"{filename}{original_extension}"
        else:
            # Extensão temporária, será atualizada após verificação do content-type
            filename = f"{filename}.bin"

    # Certificar-se de que não há caracteres inválidos
    filename = re.sub(r'[\\/*?:"<>|]', '_', filename)

    local_path = os.path.join(docs_dir, filename)

    # Verificar se o arquivo já existe
    if os.path.exists(local_path):
        file_extension = os.path.splitext(filename)[1].lstrip('.')
        if not file_extension:
            file_extension = "documento"
        return True, local_path, file_extension

    # Implementar sistema de retry
    for attempt in range(MAX_RETRIES):
        try:
            # Usar um timeout específico para download
            timeout = aiohttp.ClientTimeout(
                total=DOWNLOAD_TIMEOUT,
                sock_connect=CONNECT_TIMEOUT,
                sock_read=DOWNLOAD_TIMEOUT
            )

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': '*/*',
            }

            logger.info(
                f"Iniciando download de: {clean_url} (tentativa {attempt+1}/{MAX_RETRIES})")

            # Verificar rapidamente se a URL é válida antes de tentar o download completo
            try:
                async with session.head(clean_url, headers=headers, timeout=aiohttp.ClientTimeout(total=10), allow_redirects=True) as head_response:
                    if head_response.status >= 400:
                        logger.warning(
                            f"URL retornou status {head_response.status}: {clean_url}")
                        if head_response.status == 404:
                            logger.error(
                                f"Documento não encontrado (404): {clean_url}")
                        return False, "", ""
            except Exception as head_error:
                logger.warning(
                    f"Erro ao verificar cabeçalho: {str(head_error)}, tentando download direto")

            # Usar a mesma sessão que foi passada como parâmetro, que já deve ter SSL desabilitado
            async with session.get(clean_url, headers=headers, timeout=timeout, allow_redirects=True) as response:
                if response.status != 200:
                    logger.error(
                        f"Erro ao baixar {clean_url}: Status {response.status} (tentativa {attempt+1}/{MAX_RETRIES})")
                    # Se for 404, não tentar novamente
                    if response.status == 404:
                        return False, "", ""
                    # Para outros erros, tentar novamente
                    # Backoff exponencial
                    await asyncio.sleep(2 * (attempt + 1))
                    continue

                # Verificar content-type
                content_type = response.headers.get(
                    'Content-Type', '').split(';')[0].strip().lower()

                # Verificar se o Content-Type indica um arquivo binário vs texto/html
                if 'text/html' in content_type and not any(ext in filename.lower() for ext in DOCUMENT_EXTENSIONS):
                    logger.info(
                        f"Ignorando {clean_url} com Content-Type: {content_type}")
                    return False, "", ""

                # Tentar obter nome do arquivo do header Content-Disposition
                content_disposition = response.headers.get(
                    'Content-Disposition', '')
                if 'filename=' in content_disposition:
                    cd_filename = re.findall(
                        'filename=["\'](.*?)["\']', content_disposition)
                    if cd_filename:
                        new_filename = cd_filename[0]
                        if new_filename and len(new_filename) < 100:
                            # Atualizar o nome do arquivo se encontramos um válido no header
                            filename = new_filename
                            local_path = os.path.join(docs_dir, filename)

                # Atualizar extensão do arquivo com base no Content-Type
                if filename.endswith('.bin') or '.' not in filename:
                    ext = guess_file_extension(content_type, url)
                    if ext:
                        filename = os.path.splitext(filename)[0] + ext
                        local_path = os.path.join(docs_dir, filename)

                # Criar o diretório pai se não existir (para casos de subdiretórios)
                os.makedirs(os.path.dirname(local_path), exist_ok=True)

                # Salvar o conteúdo do arquivo com timeout para cada chunk
                logger.info(f"Baixando {clean_url} para {local_path}")

                # Usar um timeout para o download do conteúdo
                content_size = 0
                try:
                    with open(local_path, 'wb') as f:
                        chunk_size = 1024 * 1024  # 1MB por chunk
                        download_start_time = asyncio.get_event_loop().time()

                        while True:
                            # Verificar timeout para evitar travamentos
                            current_time = asyncio.get_event_loop().time()
                            if current_time - download_start_time > DOWNLOAD_TIMEOUT:
                                raise asyncio.TimeoutError(
                                    f"Timeout ao baixar conteúdo de {clean_url}")

                            # Ler chunk com timeout
                            try:
                                chunk = await asyncio.wait_for(
                                    response.content.read(chunk_size),
                                    timeout=30
                                )
                            except asyncio.TimeoutError:
                                logger.warning(
                                    f"Timeout ao ler chunk de {clean_url}")
                                if content_size > 0:
                                    # Se já baixou algum conteúdo, considera como sucesso parcial
                                    break
                                else:
                                    raise

                            if not chunk:
                                break

                            f.write(chunk)
                            content_size += len(chunk)
                except Exception as chunk_error:
                    logger.error(f"Erro ao baixar chunks: {str(chunk_error)}")
                    # Se baixou algum conteúdo, considera sucesso parcial
                    if content_size > 0 and os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                        logger.warning(
                            f"Download parcial para {local_path} ({content_size} bytes)")
                    else:
                        # Se não baixou nada, remover arquivo vazio e tentar novamente
                        if os.path.exists(local_path):
                            os.remove(local_path)
                        if attempt < MAX_RETRIES - 1:
                            await asyncio.sleep(2 * (attempt + 1))
                            continue
                        return False, "", ""

                # Verificar se o arquivo foi salvo e tem conteúdo
                if not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
                    logger.error(
                        f"Arquivo baixado está vazio ou não existe: {local_path}")
                    if os.path.exists(local_path):
                        os.remove(local_path)
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(2 * (attempt + 1))
                        continue
                    return False, "", ""

                logger.info(
                    f"Download concluído: {local_path} ({os.path.getsize(local_path)} bytes)")

                # Determinar o tipo de arquivo
                file_extension = os.path.splitext(filename)[1].lstrip('.')

                if not file_extension:
                    # Se não tem extensão, derivar do content-type
                    if 'pdf' in content_type:
                        file_type = 'pdf'
                    elif 'excel' in content_type or 'spreadsheet' in content_type:
                        file_type = 'excel'
                    elif 'word' in content_type:
                        file_type = 'word'
                    elif 'csv' in content_type:
                        file_type = 'csv'
                    elif 'powerpoint' in content_type or 'presentation' in content_type:
                        file_type = 'powerpoint'
                    elif 'zip' in content_type or 'compressed' in content_type:
                        file_type = 'arquivo_compactado'
                    else:
                        file_type = 'documento'
                else:
                    file_type = file_extension

                return True, local_path, file_type

        except asyncio.TimeoutError:
            logger.error(
                f"Timeout ao baixar {url} (tentativa {attempt+1}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES - 1:
                # Espera antes de tentar novamente
                await asyncio.sleep(2 * (attempt + 1))
            else:
                return False, "", ""
        except aiohttp.ClientError as ce:
            logger.error(
                f"Erro de cliente ao baixar {url}: {str(ce)} (tentativa {attempt+1}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(2 * (attempt + 1))
            else:
                return False, "", ""
        except Exception as e:
            logger.error(
                f"Erro ao baixar {url}: {str(e)} (tentativa {attempt+1}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(2 * (attempt + 1))
            else:
                return False, "", ""

    # Se chegou aqui, todas as tentativas falharam
    return False, "", ""

# =============================================================================
# FUNÇÕES PRINCIPAIS DE PROCESSAMENTO
# =============================================================================


async def extract_zip_file(zip_path: str, extract_dir: str, nome_empresa: str, cur, tabela: str, parent_url: str) -> List[str]:
    """
    Extrai o conteúdo de um arquivo ZIP e salva informações sobre os arquivos extraídos no banco.

    Args:
        zip_path: Caminho para o arquivo ZIP
        extract_dir: Diretório onde extrair os arquivos
        nome_empresa: Nome da empresa para organização
        cur: Cursor do banco de dados
        tabela: Tabela onde salvar as informações
        parent_url: URL original do arquivo ZIP

    Returns:
        Lista dos caminhos dos arquivos extraídos
    """
    extracted_files = []
    zip_tags = extract_path_tags(parent_url)

    # Obter diretório de destino para arquivos extraídos
    final_extracted_dir = get_extracted_output_dir(nome_empresa)

    # Adicionar tag genérica
    if 'arquivo_compactado' not in zip_tags:
        zip_tags.append('arquivo_compactado')

    try:
        logger.info(f"Extraindo arquivo ZIP: {zip_path}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Lista dos arquivos no ZIP
            zip_contents = zip_ref.namelist()

            # Criar um UUID para este ZIP específico para relacionar os arquivos extraídos
            zip_uuid = str(uuid.uuid4())

            # Criar diretório específico para este ZIP dentro do diretório de extraídos
            zip_specific_dir = os.path.join(
                final_extracted_dir, os.path.basename(zip_path).replace('.zip', ''))
            os.makedirs(zip_specific_dir, exist_ok=True)

            # Extrair todos os arquivos primeiro para o diretório temporário
            zip_ref.extractall(extract_dir)

            # Processar cada arquivo extraído
            for item in zip_contents:
                if item.endswith('/'):  # Diretório
                    continue

                extracted_path = os.path.join(extract_dir, item)

                # Verificar se o arquivo existe após a extração
                if not os.path.exists(extracted_path):
                    logger.warning(
                        f"Arquivo extraído não encontrado: {extracted_path}")
                    continue

                # Verificar tamanho do arquivo (ignorar arquivos vazios)
                file_size = os.path.getsize(extracted_path)
                if file_size == 0:
                    logger.info(f"Ignorando arquivo vazio: {item}")
                    continue

                # Determinar tipo do arquivo
                file_type = get_file_type(item)

                # Mover o arquivo para o diretório final
                final_filename = os.path.basename(item)
                final_path = os.path.join(zip_specific_dir, final_filename)

                # Garantir que o diretório de destino existe
                os.makedirs(os.path.dirname(final_path), exist_ok=True)

                # Copiar o arquivo para o destino final
                shutil.copy2(extracted_path, final_path)
                logger.info(f"Arquivo extraído movido para: {final_path}")

                # Criar caminho relativo para armazenar no banco
                script_dir = os.path.dirname(os.path.abspath(__file__))
                project_dir = os.path.dirname(script_dir)
                rel_path = os.path.relpath(final_path, project_dir)

                # Criar tags para o arquivo extraído
                file_tags = list(zip_tags)  # Copiar as tags do ZIP pai
                file_tags.append('extraido_de_zip')
                file_tags.append(file_type)

                # Nome do arquivo para exibição
                filename = os.path.basename(item)

                # Criar link virtual para referenciar o arquivo extraído
                virtual_link = f"{parent_url}#extracted/{zip_uuid}/{filename}"

                # Criar conteúdo markdown com informações sobre o arquivo extraído
                content = f"# Arquivo extraído: {filename}\n\n"
                content += f"**Tipo:** {file_type}\n\n"
                content += f"**Extraído de:** [{os.path.basename(zip_path)}]({parent_url})\n\n"
                content += f"**Caminho dentro do ZIP:** {item}\n\n"
                content += f"**Arquivo local:** {rel_path}\n\n"
                content += f"**Tamanho:** {file_size} bytes\n\n"

                # Inserir no banco de dados
                try:
                    # Insert - os arquivos extraídos são sempre novos registros
                    cur.execute(f"""
                        INSERT INTO {tabela} (content, link, local_path, tags, parent_document)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (content, virtual_link, rel_path, file_tags, parent_url))

                    logger.info(f"Arquivo extraído salvo no banco: {item}")
                    extracted_files.append(final_path)

                except Exception as e:
                    logger.error(
                        f"Erro ao salvar arquivo extraído no banco: {str(e)}")
                    # Não fazer rollback aqui para não afetar outras operações

        return extracted_files

    except zipfile.BadZipFile:
        logger.error(f"Arquivo ZIP corrompido ou inválido: {zip_path}")
        return []
    except Exception as e:
        logger.error(f"Erro ao extrair arquivo ZIP {zip_path}: {str(e)}")
        return []

# =============================================================================
# FUNÇÃO PRINCIPAL DE PROCESSAMENTO
# =============================================================================


async def process_empresa(nome_empresa: str, force_update: bool = False, skip_browser: bool = False, use_cache: bool = True) -> None:
    """
    Processa o crawler para uma empresa específica.

    Args:
        nome_empresa: Nome da empresa a ser processada
        force_update: Se deve forçar atualização de todas as páginas
        skip_browser: Se deve usar detecção antecipada de arquivos e pular navegação por browser
        use_cache: Se deve usar o sistema de cache para URLs já processadas
    """
    if nome_empresa not in EMPRESAS:
        logger.error(
            f"Empresa {nome_empresa} não encontrada nas configurações.")
        return

    config = EMPRESAS[nome_empresa]
    logger.info(f"Iniciando crawler para {nome_empresa.upper()}...")

    # Registrar adaptador para listas
    register_adapter(list, adapt_list)

    # Obter URLs já processadas do BD e do cache
    existing_urls = set()
    if not force_update:
        # Carregar URLs do banco de dados
        existing_urls = get_existing_urls(config["tabela"])
        logger.info(
            f"Encontradas {len(existing_urls)} URLs já processadas no banco para {nome_empresa}")

        # Se uso de cache estiver habilitado, adicionar também as URLs do cache
        if use_cache:
            cache_urls = load_crawled_urls_from_cache(nome_empresa)
            existing_urls.update(cache_urls)
            logger.info(
                f"Adicionadas {len(cache_urls)} URLs do cache para {nome_empresa}")

    # Lista para armazenar URLs processadas nesta execução
    crawled_urls = set()

    # Lista para armazenar URLs de documentos que encontramos
    document_urls = []

    # Carregar documentos já processados do cache para evitar reprocessamento
    if use_cache and not force_update:
        cached_documents = load_documents_from_cache(nome_empresa)
        logger.info(
            f"Carregados {len(cached_documents)} documentos do cache para {nome_empresa}")
        # Remover documentos já processados da lista de processar
        existing_urls.update(cached_documents)

    # Configurações do browser com modo verboso
    browser_config = BrowserConfig(verbose=True)

    # Verificar se a empresa precisa de configuração especial simplificada
    use_simplified = config.get("use_simplified_crawler", False)

    # Configuração avançada do crawler - personalizada para cada empresa se necessário
    if use_simplified and nome_empresa == "ceitec":
        # Usar configuração simples para CEITEC baseada no script que funcionava
        run_config = CrawlerRunConfig(
            deep_crawl_strategy=BFSDeepCrawlStrategy(
                max_depth=config["max_depth"],
                include_external=config["include_external"],
                max_pages=10000
            ),
            # Configurações mínimas que funcionavam no script antigo
            word_count_threshold=10,
            exclude_external_links=True,
            remove_overlay_elements=True,
            process_iframes=True,
            cache_mode=CacheMode.DISABLED,
        )
        logger.info(f"Usando configuração simplificada para {nome_empresa}")
    else:
        # Usar configuração padrão para as outras empresas
        run_config = CrawlerRunConfig(
            deep_crawl_strategy=BFSDeepCrawlStrategy(
                max_depth=config["max_depth"],
                include_external=config["include_external"],
                max_pages=10000
            ),
            # Configurações de limpeza de conteúdo
            word_count_threshold=10,  # Mínimo de palavras por bloco de conteúdo
            excluded_tags=config["excluded_tags"],  # Tags para excluir
            # Múltiplos seletores separados por vírgula
            excluded_selector=config["excluded_selector"],
            exclude_external_links=True,  # Remover links externos
            remove_overlay_elements=True,  # Remover popups/modals
            process_iframes=True,  # Processar conteúdo de iframes
            cache_mode=CacheMode.DISABLED,  # Desabilitar completamente o cache
        )

    # Lista para armazenar URLs de documentos que encontramos
    document_urls = []

    # Lista para armazenar URLs com alta probabilidade de serem documentos
    probable_document_urls = set()

    # Desativar verificação SSL independentemente da opção --no-ssl-verify
    # para garantir consistência em todas as requisições
    ssl_context = None  # None será tratado como ssl=False nas requisições

    # Verificar se a empresa tem configuração específica para ignorar erros de SSL
    ignore_ssl = config.get("ignore_ssl_errors", False)

    # Criar uma sessão HTTP para download de documentos com SSL desativado
    conn_timeout = aiohttp.ClientTimeout(
        total=3600)  # 1 hora para a sessão completa

    # Usar TCPConnector com verify_ssl=False em vez de ssl=False
    connector = aiohttp.TCPConnector(verify_ssl=False)

    # Para a CEITEC, vamos usar configurações de conexão mais tolerantes
    if nome_empresa == "ceitec":
        connector = aiohttp.TCPConnector(
            verify_ssl=False,
            force_close=True,  # Forçar fechamento de conexões para evitar problemas
            enable_cleanup_closed=True,  # Limpar conexões fechadas
            limit=10  # Limitar número de conexões paralelas
        )
        logger.info(
            f"Usando configuração de conexão otimizada para {nome_empresa}")

    # Usar timeout mais resistente a travamentos
    timeout = aiohttp.ClientTimeout(
        total=GLOBAL_TIMEOUT,  # Timeout global para toda a sessão
        connect=CONNECT_TIMEOUT,  # Timeout para estabelecer conexão
        sock_connect=CONNECT_TIMEOUT,  # Timeout para conectar socket
        sock_read=DOWNLOAD_TIMEOUT  # Timeout para ler dados do socket
    )

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Função para checagem rápida de URLs de documentos quando skip_browser=True
        async def check_document_head(url: str) -> bool:
            """Faz uma requisição HEAD para verificar se o URL é um documento sem baixá-lo"""
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                async with session.head(url, headers=headers, timeout=30, allow_redirects=True) as response:
                    if response.status != 200:
                        return False

                    # Verificar pelo content-type
                    content_type = response.headers.get(
                        'Content-Type', '').lower()
                    if any(doc_type in content_type for doc_type in [
                        'pdf', 'msword', 'document', 'excel', 'spreadsheet',
                        'powerpoint', 'presentation', 'zip', 'compressed',
                        'octet-stream'
                    ]):
                        return True

                    # Verificar pela extensão da URL
                    _, ext = os.path.splitext(urlparse(url).path.lower())
                    if ext in DOCUMENT_EXTENSIONS:
                        return True

                    return False
            except Exception as e:
                logger.debug(f"Erro ao verificar HEAD {url}: {str(e)}")
                return False

        # Antes de executar o crawler, tente obter links da página inicial
        # para CEITEC, vamos pular esta etapa conforme o script antigo
        if not (nome_empresa == "ceitec" and use_simplified):
            try:
                logger.info(
                    f"Fazendo análise prévia da página inicial de {nome_empresa}")
                # Usar um cliente HTTP com SSL desabilitado, mas de forma correta
                connector = aiohttp.TCPConnector(verify_ssl=False)
                async with aiohttp.ClientSession(connector=connector) as ssl_disabled_session:
                    async with ssl_disabled_session.get(config["url"], timeout=30) as response:
                        if response.status == 200:
                            html_content = await response.text()
                            initial_links = await extract_links_from_page(html_content, config["url"])

                            # Verificar quais links são documentos
                            for link in initial_links:
                                if is_definitely_document_url(link):
                                    probable_document_urls.add(link)
                                    logger.info(
                                        f"Documento identificado na página inicial: {link}")
            except Exception as e:
                logger.error(
                    f"Erro na análise prévia da página inicial: {str(e)}")

        # Filtro para URLs já processadas e para identificar documentos
        async def should_process_url(url: str) -> bool:
            if not url or not isinstance(url, str):
                return False

            try:
                # Prevenir que URLs inválidas sejam processadas
                if not url.startswith(('http://', 'https://')):
                    return False

                # Verificação rigorosa para determinar se é um documento
                if is_definitely_document_url(url):
                    logger.info(f"Evitando navegação para documento: {url}")
                    # Adicionar à lista de documentos para download direto
                    if url not in document_urls and (force_update or url not in existing_urls):
                        document_urls.append(url)
                    return False  # NUNCA processar URLs que são definitivamente documentos

                # Verificação adicional para extensões de arquivo conhecidas
                parsed_url = urlparse(url)
                path = parsed_url.path.lower()
                if any(path.endswith(ext) for ext in DOCUMENT_EXTENSIONS):
                    logger.info(
                        f"Evitando navegação para arquivo com extensão conhecida: {url}")
                    if url not in document_urls and (force_update or url not in existing_urls):
                        document_urls.append(url)
                    return False

                # Verificação rápida para tipos comuns de documentos em URLs
                doc_keywords = ['pdf', 'doc', 'docx',
                                'xls', 'xlsx', 'ppt', 'pptx', 'zip']
                if any(keyword in path for keyword in doc_keywords):
                    logger.info(
                        f"Evitando navegação para possível documento: {url}")
                    if url not in document_urls and (force_update or url not in existing_urls):
                        document_urls.append(url)
                    return False

                # Verificar cabeçalhos da URL antes de navegar para confirmar que não é um documento
                if skip_browser:
                    try:
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                        }
                        # Usar a sessão existente que já tem SSL desabilitado corretamente
                        head_response = await session.head(url, headers=headers, timeout=10, allow_redirects=True)

                        # Verificar content-type
                        content_type = head_response.headers.get(
                            'Content-Type', '').lower()

                        # Se o content-type indicar que é um documento, não navegar
                        if any(doc_type in content_type for doc_type in [
                            'pdf', 'msword', 'document', 'excel', 'spreadsheet',
                            'powerpoint', 'presentation', 'zip', 'compressed',
                            'octet-stream', 'binary'
                        ]):
                            logger.info(
                                f"Evitando navegação baseado no Content-Type {content_type}: {url}")
                            if url not in document_urls and (force_update or url not in existing_urls):
                                document_urls.append(url)
                            return False
                    except Exception as e:
                        # Em caso de erro, prosseguir com a navegação mas registrar o problema
                        logger.debug(
                            f"Erro ao verificar cabeçalhos de {url}: {str(e)}")

                # Verificação mais ampla para possíveis documentos
                if url in probable_document_urls or is_document_url(url):
                    if url not in document_urls and (force_update or url not in existing_urls):
                        document_urls.append(url)
                    return False  # Não processar URLs que provavelmente são documentos

                # Não processar URLs já baixadas anteriormente, a menos que force_update seja True
                if not force_update and url in existing_urls:
                    return False

                # Processar URLs que não são documentos e que não estão em existing_urls (ou force_update é True)
                return True
            except Exception as e:
                logger.error(f"Erro no filtro de URL {url}: {str(e)}")
                return False  # Em caso de erro, não processar a URL

        # Modificar o crawler para interceptar solicitações ao browser
        async with AsyncWebCrawler(config=browser_config) as crawler:
            # Adicionar o filtro de URLs ao crawler
            if nome_empresa == "ceitec" and use_simplified:
                # Para CEITEC, desativamos o filtro de URL completamente, como no script original
                crawler.url_filter = None
                logger.info(f"Desativando filtro de URL para {nome_empresa}")
            else:
                # Para outras empresas, usar o filtro normal
                crawler.url_filter = should_process_url

            # Executar o crawler para páginas HTML
            results = await crawler.arun(config["url"], config=run_config)

            # Adicionar URLs processadas à lista para salvar no cache
            for result in results:
                if result.success:
                    crawled_urls.add(result.url)

            logger.info(
                f"Crawled {len(results)} páginas HTML para {nome_empresa}")

            # Conectar ao banco de dados
            conn = get_db_connection()
            cur = conn.cursor()

            novas_paginas = 0
            atualizadas = 0

            # Processar os resultados do crawler (páginas HTML)
            for result in results:
                if not result.success:
                    logger.error(
                        f"Falha ao crawlear {result.url}: {result.error_message}")
                    continue

                # Verificar links na página para encontrar documentos adicionais
                if hasattr(result, 'links') and result.links:
                    # Extrair links de documentos da estrutura de links
                    if isinstance(result.links, dict):
                        all_links = []
                        if "internal" in result.links:
                            all_links.extend([link.get("href") for link in result.links["internal"]
                                             if isinstance(link, dict) and "href" in link])
                        if "external" in result.links:
                            all_links.extend([link.get("href") for link in result.links["external"]
                                             if isinstance(link, dict) and "href" in link])

                        # Adicionar links para documentos à lista
                        for link in all_links:
                            if link and is_document_url(link) and link not in document_urls and link not in existing_urls:
                                document_urls.append(link)
                                logger.info(f"Documento encontrado: {link}")

                # Também extrair do HTML bruto para caso a estrutura de links falhe
                if hasattr(result, 'html'):
                    raw_links = await extract_links_from_page(result.html, result.url)
                    for link in raw_links:
                        if link and is_document_url(link) and link not in document_urls and link not in existing_urls:
                            document_urls.append(link)
                            logger.info(
                                f"Documento encontrado (via HTML): {link}")

                # Preparar o conteúdo em markdown
                content = ""
                if hasattr(result, 'markdown'):
                    if isinstance(result.markdown, str):
                        content = result.markdown
                    elif hasattr(result.markdown, 'raw_markdown'):
                        content = result.markdown.raw_markdown
                    else:
                        content = str(result.markdown)
                elif hasattr(result, 'title') and result.title:
                    # Se não tem markdown mas tem título, criar estrutura básica
                    content = f"# {result.title}\n\n"
                    if hasattr(result, 'text'):
                        content += result.text
                    elif hasattr(result, 'cleaned_html'):
                        # Tentar converter HTML para markdown usando uma representação simples
                        content += f"```html\n{result.cleaned_html}\n```"
                elif hasattr(result, 'text'):
                    content = result.text
                else:
                    content = "Sem conteúdo disponível"

                # Extrair imagens e adicionar ao conteúdo markdown
                images = []
                if hasattr(result, 'media') and isinstance(result.media, dict) and "images" in result.media:
                    content += "\n\n## Imagens\n\n"
                    for image in result.media["images"]:
                        if isinstance(image, dict) and "src" in image:
                            src = image["src"]
                            alt = image.get("alt", "Imagem")
                            content += f"![{alt}]({src})\n\n"
                            # Adiciona URL da imagem à lista
                            images.append(src)

                # Se tiver links, adicionar seção de links no markdown
                if hasattr(result, 'links') and result.links:
                    content += "\n\n## Links\n\n"
                    if isinstance(result.links, dict):
                        if "internal" in result.links:
                            content += "### Links Internos\n\n"
                            for link in result.links["internal"]:
                                if isinstance(link, dict) and "href" in link:
                                    text = link.get("text", link["href"])
                                    content += f"- [{text}]({link['href']})\n"
                        if "external" in result.links:
                            content += "\n### Links Externos\n\n"
                            for link in result.links["external"]:
                                if isinstance(link, dict) and "href" in link:
                                    text = link.get("text", link["href"])
                                    content += f"- [{text}]({link['href']})\n"
                    else:
                        for link in result.links:
                            content += f"- {link}\n"

                # Salvar o conteúdo HTML original
                html_content = ""
                if hasattr(result, 'html'):
                    html_content = result.html
                else:
                    # Fallback caso não tenha HTML
                    html_content = f"<html><head><title>{getattr(result, 'title', 'Sem título')}</title></head><body>"
                    if hasattr(result, 'cleaned_html'):
                        html_content += result.cleaned_html
                    elif hasattr(result, 'text'):
                        html_content += f"<pre>{result.text}</pre>"
                    else:
                        html_content += "<p>Sem conteúdo disponível</p>"
                    html_content += "</body></html>"

                # Salvar o HTML
                html_path = save_html_content(
                    result.url, html_content, nome_empresa)
                logger.info(f"Conteúdo HTML salvo em: {html_path}")

                # Extrair tags do caminho da URL
                tags = extract_path_tags(result.url)

                # Se não houver tags do caminho, usar subdomínio
                if not tags:
                    subdomain = extract_subdomain(result.url)
                    if subdomain and subdomain != 'www':
                        tags.append(subdomain)

                # Remover tags vazias e converter para array PostgreSQL
                tags = [tag for tag in tags if tag]
                tags_array = tags if tags else None

                # Inserir ou atualizar no banco
                try:
                    # Verificar se o link já existe no banco de dados
                    cur.execute(
                        f"SELECT id FROM {config['tabela']} WHERE link = %s", (result.url,))
                    existing_record = cur.fetchone()

                    if existing_record:
                        # Update - esta parte só será executada se force_update=True,
                        # caso contrário a URL seria filtrada antes
                        cur.execute(f"""
                            UPDATE {config['tabela']} 
                            SET content = %s,
                                images = %s,
                                tags = %s,
                                dt_download = CURRENT_TIMESTAMP,
                                local_path = %s
                            WHERE link = %s
                        """, (content, images if images else None, tags_array, html_path, result.url))
                        atualizadas += 1
                    else:
                        # Insert - para páginas novas
                        cur.execute(f"""
                            INSERT INTO {config['tabela']} (content, link, images, tags, local_path)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (content, result.url, images if images else None, tags_array, html_path))
                        novas_paginas += 1

                    conn.commit()
                except Exception as e:
                    logger.error(f"Erro ao salvar no banco: {str(e)}")
                    conn.rollback()

            # Processar documentos encontrados
            logger.info(
                f"Processando {len(document_urls)} documentos encontrados para {nome_empresa}...")
            documentos_baixados = 0
            documentos_falhos = 0
            documentos_extraidos = 0

            # Adicionar contador para monitoramento de progresso
            # Reportar a cada 10% aprox.
            progress_interval = max(1, len(document_urls) // 10)

            # Criar chunks de URLs de documentos para processar em paralelo
            chunk_size = 3  # Processar menos documentos por vez para melhor controle

            # Criar diretório temporário para extração
            script_dir = os.path.dirname(os.path.abspath(__file__))
            project_dir = os.path.dirname(script_dir)
            temp_extract_dir = os.path.join(
                get_output_dir(nome_empresa), 'temp_extracted')
            os.makedirs(temp_extract_dir, exist_ok=True)

            # Criar chunks de URLs de documentos para processar em paralelo
            for i in range(0, len(document_urls), chunk_size):
                # Mostrar progresso
                if i % progress_interval == 0 or i + chunk_size >= len(document_urls):
                    logger.info(
                        f"Progresso de download: {i}/{len(document_urls)} documentos ({int(i/len(document_urls)*100 if document_urls else 0)}%)")

                chunk = document_urls[i:i+chunk_size]
                download_tasks = []

                for doc_url in chunk:
                    # Verificar se o documento já foi processado anteriormente e não estamos forçando atualização
                    if not force_update:
                        cur.execute(
                            f"SELECT id FROM {config['tabela']} WHERE link = %s", (doc_url,))
                        if cur.fetchone():
                            continue

                    # Criar task para download
                    download_tasks.append(download_document(
                        doc_url, nome_empresa, session))

                # Executar os downloads em paralelo com timeout global
                if download_tasks:
                    try:
                        # Adicionar timeout global para o gather
                        download_results = await asyncio.wait_for(
                            asyncio.gather(*download_tasks,
                                           return_exceptions=True),
                            timeout=DOWNLOAD_TIMEOUT * 2  # Dar tempo extra para o conjunto de downloads
                        )

                        # Processar resultados dos downloads
                        for idx, result in enumerate(download_results):
                            if idx >= len(chunk):  # Verificação de segurança
                                continue

                            doc_url = chunk[idx]

                            # Verificar se ocorreu uma exceção
                            if isinstance(result, Exception):
                                logger.error(
                                    f"Erro ao baixar {doc_url}: {str(result)}")
                                documentos_falhos += 1
                                continue

                            success, local_path, file_type = result

                            if not success:
                                logger.error(
                                    f"Falha ao baixar documento: {doc_url}")
                                documentos_falhos += 1
                                continue

                            # Criar tags para o documento
                            doc_tags = extract_path_tags(doc_url)
                            if not doc_tags:
                                subdomain = extract_subdomain(doc_url)
                                if subdomain and subdomain != 'www':
                                    doc_tags.append(subdomain)

                            # Adicionar tag do tipo de arquivo
                            doc_tags.append(file_type)
                            # Tag genérica para todos os documentos
                            doc_tags.append('documento')

                            # Se tiver "transparencia" na URL, adicionar tag
                            if 'transparencia' in doc_url.lower():
                                doc_tags.append('transparencia')

                            # Caminho relativo para armazenar no banco de dados
                            script_dir = os.path.dirname(
                                os.path.abspath(__file__))
                            project_dir = os.path.dirname(script_dir)
                            rel_path = os.path.relpath(local_path, project_dir)

                            # Obter nome do arquivo para título
                            filename = os.path.basename(local_path)

                            # Criar conteúdo markdown com informações sobre o documento
                            content = f"# Documento: {filename}\n\n"
                            content += f"**Tipo:** {file_type}\n\n"
                            content += f"**Link original:** {doc_url}\n\n"
                            content += f"**Arquivo local:** {rel_path}\n\n"

                            # Inserir ou atualizar no banco
                            try:
                                # Verificar se o link já existe
                                cur.execute(
                                    f"SELECT id FROM {config['tabela']} WHERE link = %s", (doc_url,))
                                existing_record = cur.fetchone()

                                if existing_record:
                                    # Update
                                    cur.execute(f"""
                                        UPDATE {config['tabela']} 
                                        SET content = %s,
                                            local_path = %s,
                                            tags = %s,
                                            dt_download = CURRENT_TIMESTAMP
                                        WHERE link = %s
                                    """, (content, rel_path, doc_tags, doc_url))
                                else:
                                    # Insert
                                    cur.execute(f"""
                                        INSERT INTO {config['tabela']} (content, link, local_path, tags)
                                        VALUES (%s, %s, %s, %s)
                                    """, (content, doc_url, rel_path, doc_tags))

                                conn.commit()
                                documentos_baixados += 1
                                logger.info(
                                    f"Documento salvo: {doc_url} -> {rel_path}")

                                # Se for um arquivo ZIP, extrair seu conteúdo
                                if file_type == 'zip' or local_path.lower().endswith('.zip'):
                                    try:
                                        # Criar diretório específico para este ZIP
                                        zip_extract_dir = os.path.join(
                                            temp_extract_dir, os.path.splitext(filename)[0])
                                        os.makedirs(
                                            zip_extract_dir, exist_ok=True)

                                        # Extrair o arquivo ZIP
                                        extracted_files = await extract_zip_file(
                                            local_path,
                                            zip_extract_dir,
                                            nome_empresa,
                                            cur,
                                            config['tabela'],
                                            doc_url
                                        )

                                        # Commit para salvar os arquivos extraídos
                                        conn.commit()
                                        documentos_extraidos += len(
                                            extracted_files)
                                    except Exception as e:
                                        logger.error(
                                            f"Erro ao extrair ZIP {filename}: {str(e)}")

                            except Exception as e:
                                logger.error(
                                    f"Erro ao salvar documento no banco: {str(e)}")
                                conn.rollback()
                    except asyncio.TimeoutError:
                        logger.error(
                            f"Timeout global ao processar batch de downloads")
                    except Exception as e:
                        logger.error(
                            f"Erro ao processar batch de downloads: {str(e)}")

                # Adicionar pequena pausa entre batches para evitar sobrecarga
                await asyncio.sleep(0.5)

        # ...existing code...

# =============================================================================
# FUNÇÃO PRINCIPAL E PONTO DE ENTRADA
# =============================================================================


async def main():
    # Declaração global deve vir antes de qualquer uso da variável
    global DOWNLOAD_TIMEOUT

    parser = argparse.ArgumentParser(
        description='Crawler incremental para empresas')
    parser.add_argument('--empresas', nargs='+', choices=list(EMPRESAS.keys()) + ['todas'],
                        default=['todas'], help='Empresas para processar (imbel, ceitec, telebras ou todas)')
    parser.add_argument('--force', action='store_true',
                        help='Forçar atualização de todas as páginas, mesmo as já processadas')
    parser.add_argument('--sequential', action='store_true',
                        help='Processar empresas sequencialmente em vez de em paralelo')
    parser.add_argument('--skip-browser', action='store_true',
                        help='Pular navegação por browser para arquivos de documentos')
    parser.add_argument('--no-ssl-verify', action='store_true',
                        help='Desabilitar verificação de certificados SSL')
    parser.add_argument('--no-cache', action='store_true',
                        help='Desabilitar uso de cache para URLs já processadas')
    parser.add_argument('--timeout', type=int, default=DOWNLOAD_TIMEOUT,
                        help=f'Timeout em segundos para downloads (padrão: {DOWNLOAD_TIMEOUT})')

    args = parser.parse_args()

    # Se solicitado, desabilitar verificações SSL
    if args.no_ssl_verify:
        import ssl
        # Criar contexto SSL sem verificação
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        # Configurar aiohttp para usar este contexto
        import aiohttp
        aiohttp.ClientSession._create_connection_params = lambda self, *args, **kwargs: {
            **kwargs, 'ssl': False
        }

    # Determinar quais empresas processar
    empresas_para_processar = list(
        EMPRESAS.keys()) if 'todas' in args.empresas else args.empresas

    # Usar cache por padrão, a menos que --no-cache seja especificado
    use_cache = not args.no_cache

    # Atualizar timeout se especificado
    if args.timeout and args.timeout > 0:
        DOWNLOAD_TIMEOUT = args.timeout
        logger.info(
            f"Timeout para downloads definido como {DOWNLOAD_TIMEOUT} segundos")

    # Processar as empresas
    if args.sequential:
        # Modo sequencial
        for empresa in empresas_para_processar:
            await process_empresa(empresa, args.force, skip_browser=args.skip_browser, use_cache=use_cache)
            logger.info(
                f"Concluído processamento sequencial de {empresa.upper()}")
    else:
        # Modo paralelo (processamento assíncrono de todas as empresas)
        logger.info(
            f"Iniciando processamento paralelo de {len(empresas_para_processar)} empresas")
        tasks = [process_empresa(empresa, args.force, skip_browser=args.skip_browser, use_cache=use_cache)
                 for empresa in empresas_para_processar]
        await asyncio.gather(*tasks)
        logger.info("Concluído processamento paralelo de todas as empresas")

if __name__ == "__main__":
    # Certificar-se de que o diretório de dados e saída existe
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(script_dir)
    data_dir = os.path.join(project_dir, 'data')
    output_dir = os.path.join(project_dir, 'output')
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    # Inicializar mimetypes
    mimetypes.init()

    # Adicionar mapeamentos para extensões comuns que possam faltar
    mimetypes.add_type('application/pdf', '.pdf')
    mimetypes.add_type(
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document', '.docx')
    mimetypes.add_type(
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', '.xlsx')
    mimetypes.add_type(
        'application/vnd.openxmlformats-officedocument.presentationml.presentation', '.pptx')
    mimetypes.add_type('application/msword', '.doc')
    mimetypes.add_type('application/vnd.ms-excel', '.xls')
    mimetypes.add_type('application/vnd.ms-powerpoint', '.ppt')
    mimetypes.add_type('text/csv', '.csv')
    mimetypes.add_type('application/zip', '.zip')
    mimetypes.add_type('application/x-rar-compressed', '.rar')
    mimetypes.add_type('application/x-7z-compressed', '.7z')
    # Adicionar mapas para outros formatos comuns de documentos
    mimetypes.add_type('application/vnd.ms-outlook', '.msg')
    mimetypes.add_type('application/octet-stream', '.bin')
    mimetypes.add_type('application/xml', '.xml')
    mimetypes.add_type('application/json', '.json')
    mimetypes.add_type('image/jpeg', '.jpg')
    mimetypes.add_type('image/jpeg', '.jpeg')
    mimetypes.add_type('image/png', '.png')
    mimetypes.add_type('image/gif', '.gif')
    mimetypes.add_type('audio/mpeg', '.mp3')
    mimetypes.add_type('video/mp4', '.mp4')
    mimetypes.add_type('audio/wav', '.wav')

    # Desabilitar verificações SSL definitivamente para todo o script
    import ssl
    ssl._create_default_https_context = ssl._create_unverified_context

    # Reimplementar o monkey patch correto para aiohttp
    original_aiohttp_TCPConnector = aiohttp.TCPConnector

    class PatchedTCPConnector(original_aiohttp_TCPConnector):
        def __init__(self, *args, **kwargs):
            kwargs['verify_ssl'] = False
            super().__init__(*args, **kwargs)

    # Substituir o conector original pelo nosso conector que sempre desativa SSL
    aiohttp.TCPConnector = PatchedTCPConnector

    # Também garantir que ClientSession sempre use nosso conector
    original_ClientSession = aiohttp.ClientSession

    def patched_ClientSession(*args, **kwargs):
        if 'connector' not in kwargs:
            kwargs['connector'] = PatchedTCPConnector()
        return original_ClientSession(*args, **kwargs)

    aiohttp.ClientSession = patched_ClientSession

    # Configurar tratamento de exceções não tratadas
    def exception_handler(loop, context):
        exception = context.get('exception')
        logger.error(f"Exceção não tratada: {context['message']}")
        if exception:
            logger.error(f"Detalhes: {str(exception)}")

    # Obter o event loop e configurar handler
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(exception_handler)

    # Executar o programa principal
    loop.run_until_complete(main())
    loop.close()
