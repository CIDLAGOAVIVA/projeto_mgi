import asyncio
import os
import re
from pathlib import Path
from crawl4ai import AsyncWebCrawler
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


async def main():
    # Configurações do browser com modo verboso
    browser_config = BrowserConfig(verbose=True)

    # Configuração avançada do crawler
    run_config = CrawlerRunConfig(
        deep_crawl_strategy=BFSDeepCrawlStrategy(
            max_depth=1,
            include_external=False,
            max_pages=50
        ),
        # Configurações de limpeza de conteúdo
        word_count_threshold=10,  # Mínimo de palavras por bloco de conteúdo
        excluded_tags=['form', 'header', 'footer', 'nav'],  # Tags para excluir
        exclude_external_links=True,  # Remover links externos
        remove_overlay_elements=True,  # Remover popups/modals
        process_iframes=True,  # Processar conteúdo de iframes
    )

    # Criar a pasta para armazenar os arquivos markdown
    output_dir = Path("data/IMBEL")
    output_dir.mkdir(parents=True, exist_ok=True)

    async with AsyncWebCrawler(config=browser_config) as crawler:
        results = await crawler.arun("https://www.imbel.gov.br/", config=run_config)

        print(f"Crawled {len(results)} pages in total")

        # Salvar os resultados em arquivos markdown
        for i, result in enumerate(results):
            # Verificar se o crawl foi bem-sucedido
            if not result.success:
                print(
                    f"Falha ao crawlear {result.url}: {result.error_message}")
                continue

            url = result.url
            depth = result.metadata.get('depth', 0)

            # Extrair conteúdo em markdown de forma limpa
            markdown_content = "Sem conteúdo disponível"

            # Tentar obter o conteúdo em markdown
            if hasattr(result, 'markdown'):
                if isinstance(result.markdown, str):
                    # Se markdown for uma string direta
                    markdown_content = result.markdown
                else:
                    # Se for um objeto com atributos
                    if hasattr(result.markdown, 'raw_markdown') and result.markdown.raw_markdown:
                        markdown_content = result.markdown.raw_markdown
            elif hasattr(result, 'cleaned_html') and result.cleaned_html:
                # Se não houver markdown mas tiver HTML limpo
                markdown_content = f"```html\n{result.cleaned_html}\n```"
            elif hasattr(result, 'text') and result.text:
                # Como último recurso, usar o texto bruto
                markdown_content = result.text

            # Criar um nome de arquivo baseado na URL
            filename = f"{i+1:03d}_{sanitize_filename(url)}.md"
            filepath = output_dir / filename

            # Extrair título da página
            title = "Sem título"
            if hasattr(result, 'title') and result.title:
                title = result.title
            elif hasattr(result, 'metadata') and 'title' in result.metadata:
                title = result.metadata['title']

            # Escrever no arquivo markdown
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(f"# {title}\n\n")
                f.write(f"URL: {url}\n\n")
                f.write(f"Profundidade: {depth}\n\n")
                f.write("## Conteúdo\n\n")
                f.write(markdown_content)

                # Adicionar links encontrados se disponíveis
                if hasattr(result, 'links'):
                    f.write("\n\n## Links\n\n")
                    if isinstance(result.links, dict):
                        # Se links for um dicionário com internos/externos
                        if "internal" in result.links:
                            f.write("### Links Internos\n\n")
                            for link in result.links["internal"]:
                                if isinstance(link, dict) and "href" in link:
                                    href = link["href"]
                                    text = link.get("text", href)
                                    f.write(f"- [{text}]({href})\n")
                                else:
                                    f.write(f"- {link}\n")

                        if "external" in result.links:
                            f.write("\n### Links Externos\n\n")
                            for link in result.links["external"]:
                                if isinstance(link, dict) and "href" in link:
                                    href = link["href"]
                                    text = link.get("text", href)
                                    f.write(f"- [{text}]({href})\n")
                                else:
                                    f.write(f"- {link}\n")
                    else:
                        # Se links for uma lista simples
                        for link in result.links:
                            f.write(f"- {link}\n")

                # Adicionar imagens encontradas se disponíveis
                if hasattr(result, 'media') and isinstance(result.media, dict) and "images" in result.media:
                    f.write("\n\n## Imagens\n\n")
                    for image in result.media["images"]:
                        if isinstance(image, dict) and "src" in image:
                            src = image["src"]
                            alt = image.get("alt", "Imagem")
                            f.write(f"![{alt}]({src})\n\n")

            print(f"Salvo: {filepath}")

if __name__ == "__main__":
    asyncio.run(main())
