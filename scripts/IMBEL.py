import asyncio
import os
import re
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import adapt, register_adapter, AsIs
from pathlib import Path
from crawl4ai import AsyncWebCrawler, CacheMode
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy
from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig


def sanitize_filename(url):
    """Converte uma URL em um nome de arquivo seguro."""
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
    """Estabelece conexão com o banco de dados."""
    return psycopg2.connect(
        dbname="mgi_raspagem",
        user="mgi",
        password="l4g04uff",
        host="localhost"
    )


def extract_subdomain(url):
    """Extrai o subdomínio de uma URL."""
    # Remove protocolo se existir
    url = re.sub(r'https?://', '', url)
    # Divide por pontos e pega todos menos o último e penúltimo (domínio principal)
    parts = url.split('.')
    if len(parts) > 2:
        return parts[0]
    return ''


def extract_path_tags(url):
    """Extrai tags do caminho da URL."""
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
    """Adapta uma lista Python para array PostgreSQL."""
    if lst is None:
        return AsIs('NULL')
    return AsIs("'{%s}'" % ','.join(str(x) for x in lst))


async def main():
    # Registrar adaptador para listas
    register_adapter(list, adapt_list)

    # Configurações do browser com modo verboso
    browser_config = BrowserConfig(verbose=True)

    # Configuração avançada do crawler
    run_config = CrawlerRunConfig(
        deep_crawl_strategy=BFSDeepCrawlStrategy(
            max_depth=2,
            include_external=False,
            max_pages=100
        ),
        # Configurações de limpeza de conteúdo
        word_count_threshold=10,  # Mínimo de palavras por bloco de conteúdo
        excluded_tags=['form', 'header', 'footer', 'nav'],  # Tags para excluir
        # Múltiplos seletores separados por vírgula
        excluded_selector=".blog-anteriores,.cookies-eu-banner",
        exclude_external_links=True,  # Remover links externos
        remove_overlay_elements=True,  # Remover popups/modals
        process_iframes=True,  # Processar conteúdo de iframes
        # Desabilitar completamente o cache para garantir que excluded_tags funcione
        cache_mode=CacheMode.DISABLED,
    )

    async with AsyncWebCrawler(config=browser_config) as crawler:
        results = await crawler.arun("https://www.imbel.gov.br/", config=run_config)

        print(f"Crawled {len(results)} pages in total")

        # Conectar ao banco de dados
        conn = get_db_connection()
        cur = conn.cursor()

        for result in results:
            if not result.success:
                print(
                    f"Falha ao crawlear {result.url}: {result.error_message}")
                continue

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
                        images.append(src)  # Adiciona URL da imagem à lista

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

            # Extrair tags do caminho da URL
            tags = extract_path_tags(result.url)

            # Se não houver tags do caminho, usar subdomínio
            if not tags:
                subdomain = extract_subdomain(result.url)
                if subdomain and subdomain != 'www':
                    tags.append(subdomain)

            # Imprimir tags para debug
            print(f"Tags extraídas para {result.url}: {tags}")

            # Remover tags vazias e converter para array PostgreSQL
            tags = [tag for tag in tags if tag]
            tags_array = tags if tags else None

            # Inserir ou atualizar no banco
            try:
                # Verificar se o link já existe
                cur.execute(
                    "SELECT id FROM tbl_paginas_imbel WHERE link = %s", (result.url,))
                existing_record = cur.fetchone()

                if existing_record:
                    # Update
                    cur.execute("""
                        UPDATE tbl_paginas_imbel 
                        SET content = %s,
                            images = %s,
                            tags = %s,
                            dt_download = CURRENT_TIMESTAMP
                        WHERE link = %s
                    """, (content, images if images else None, tags_array, result.url))
                else:
                    # Insert
                    cur.execute("""
                        INSERT INTO tbl_paginas_imbel (content, link, images, tags)
                        VALUES (%s, %s, %s, %s)
                    """, (content, result.url, images if images else None, tags_array))

                conn.commit()
                print(f"Salvo no banco: {result.url}")

            except Exception as e:
                print(f"Erro ao salvar no banco: {e}")
                conn.rollback()

        cur.close()
        conn.close()

if __name__ == "__main__":
    asyncio.run(main())
