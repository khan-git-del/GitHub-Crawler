#!/usr/bin/env python3
import asyncio
import aiohttp
import asyncpg
import os
import sys
from dataclasses import dataclass
from typing import List, Set

@dataclass(frozen=True)
class Repository:
    github_id: str
    name_with_owner: str
    star_count: int

class GitHubAPIAdapter:
    def __init__(self, token: str):
        self.token = token
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {self.token}", "User-Agent": "GitHub-Crawler/1.0"}
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def execute_graphql(self, query: str, variables: dict) -> dict:
        async with self.session.post(
            "https://api.github.com/graphql",
            json={"query": query, "variables": variables}
        ) as response:
            if response.status != 200:
                raise Exception(f"HTTP {response.status}")
            data = await response.json()
            if "errors" in data:
                raise Exception(f"GraphQL error: {data['errors'][0]['message']}")
            return data["data"]

class ProductionGitHubCrawler:
    def __init__(self, token: str):
        self.api_adapter = GitHubAPIAdapter(token)
        self.seen_repos: Set[str] = set()

    async def __aenter__(self):
        await self.api_adapter.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.api_adapter.__aexit__(exc_type, exc_val, exc_tb)

    def generate_comprehensive_queries(self) -> List[str]:
        queries = []
        star_ranges = [">=50000", "10000..49999", "5000..9999", "1000..4999", "500..999", "200..499", "100..199", "50..99", "20..49", "10..19", "5..9", "3..4", "2", "1"]
        for star_range in star_ranges:
            queries.append(f"stars:{star_range} sort:stars-desc")
        languages = ["javascript", "python", "java", "typescript", "c++", "go", "rust", "php", "c#", "swift", "kotlin", "ruby"]
        lang_star_ranges = ["50..*", "20..49", "10..19", "5..9", "1..4"]
        for lang in languages:
            for star_range in lang_star_ranges:
                queries.append(f"language:{lang} stars:{star_range}")
        topics = ["web", "api", "framework", "machine-learning", "ai", "blockchain", "security"]
        for topic in topics:
            queries.append(f"topic:{topic} stars:1..10")
        years = [2020, 2021, 2022, 2023, 2024, 2025]
        for year in years:
            quarters = 4 if year < 2025 else 3
            for q in range(1, quarters + 1):
                start_month = (q-1)*3 + 1
                end_month = start_month + 2
                queries.append(f"stars:1..5 created:{year}-{start_month:02d}-01..{year}-{end_month:02d}-28")
        for size in ["<5000", "5000..50000", ">50000"]:
            queries.append(f"size:{size} stars:1..3")
        for license_type in ["mit", "apache-2.0", "gpl-3.0"]:
            queries.append(f"license:{license_type} stars:1..5")
        queries.append("stars:1..10 pushed:>2024-01-01")
        return queries
    
    async def execute_graphql_query(self, search_query: str) -> List[Repository]:
        graphql_query = """
        query($searchQuery: String!, $cursor: String) {
            search(query: $searchQuery, type: REPOSITORY, first: 100, after: $cursor) {
                nodes { ... on Repository { id, nameWithOwner, stargazerCount } }
                pageInfo { hasNextPage, endCursor }
            }
        }
        """
        repositories = []
        cursor = None
        max_pages = 10
        while True:
            data = await self.api_adapter.execute_graphql(graphql_query, {"searchQuery": search_query, "cursor": cursor})
            nodes = data["search"]["nodes"]
            if not nodes:
                break
            for node in nodes:
                if node["id"] not in self.seen_repos:
                    self.seen_repos.add(node["id"])
                    repositories.append(Repository(
                        github_id=node["id"],
                        name_with_owner=node.get("nameWithOwner", "unknown/unknown"),
                        star_count=max(0, node.get("stargazerCount", 0))
                    ))
            if not data["search"]["pageInfo"]["hasNextPage"]:
                break
            cursor = data["search"]["pageInfo"]["endCursor"]
        return repositories
    
    async def crawl_100k_repositories(self) -> Tuple[List[Repository]]:
        all_repositories = []
        queries = self.generate_comprehensive_queries()
        random.shuffle(queries)
        semaphore = asyncio.Semaphore(5)
        async def limited_exec(query):
            async with semaphore:
                return await self.execute_graphql_query(query)
        batch_size = 10
        for batch_start in range(0, len(queries), batch_size):
            if len(all_repositories) >= 100000:
                break
            batch_queries = queries[batch_start:batch_start + batch_size]
            batch_results = await asyncio.gather(*[limited_exec(q) for q in batch_queries])
            for batch_repos in batch_results:
                all_repositories.extend(batch_repos)
        return tuple(all_repositories[:100000])

class RepositorySaver:
    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self.pool = None
    
    async def __aenter__(self):
        self.pool = await asyncpg.create_pool(self.connection_url, min_size=1, max_size=3)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.pool:
            await self.pool.close()
    
    async def save_repositories_batch(self, repositories: List[Repository]):
        if not repositories:
            return
        sql = """
        INSERT INTO repositories (github_id, name_with_owner, star_count)
        VALUES ($1, $2, $3)
        ON CONFLICT (github_id) DO UPDATE SET
            star_count = EXCLUDED.star_count,
            crawled_at = CURRENT_TIMESTAMP
        """
        async with self.pool.acquire() as conn:
            await conn.executemany(sql, [(r.github_id, r.name_with_owner, r.star_count) for r in repositories])

async def main():
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        sys.exit(1)
    database_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/github_crawler")
    async with ProductionGitHubCrawler(github_token) as crawler:
        repositories = await crawler.crawl_100k_repositories()
    async with RepositorySaver(database_url) as saver:
        batch_size = 2000
        for i in range(0, len(repositories), batch_size):
            await saver.save_repositories_batch(repositories[i:i + batch_size])

if __name__ == "__main__":
    asyncio.run(main())
