import asyncio
import os
from crawl4ai import *

async def main():
    # Create directories if they don't exist
    os.makedirs("prompts/CEITEC/dados_raspagem", exist_ok=True)
    
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(
            url="http://www.ceitec-sa.com/pt",
        )
        
        # Save the output to the specified file
        with open("prompts/CEITEC/dados_raspagem/mapa_site.md", "w", encoding="utf-8") as f:
            f.write(result.markdown)

if __name__ == "__main__":
    asyncio.run(main())
