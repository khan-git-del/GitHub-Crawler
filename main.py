#!/usr/bin/env python3
import asyncio
import aiohttp
import asyncpg
import os
import sys
from dataclasses import dataclass
from typing import List, Set

@dataclass
class Repository:
    github_id: str
    name_with_owner: str
    star_count: int

async def fetch_repos(query: str, seen_repos: Set[str]) -> List[Repository]:
    async with aiohttp.ClientSession(
        headers={"Authorization": f"Bearer {os.getenv('GITHUB_TOKEN')}", "User-Agent": "Minimal-Crawler"}
    ) as session:
        repos = []
        cursor = None
        for _ in range(5):  # 5 pages = 500 repos max
            async with session.post(
                "https://api.github.com/graphql",
                json={"query": """
                    query($q: String!, $cursor: String) {
                        search(query: $q, type: REPOSITORY, first: 100, after: $cursor) {
                            nodes { ... on Repository { id, nameWithOwner, stargazerCount } }
                            pageInfo { hasNextPage, endCursor }
                        }
                    }
                    """, "variables": {"q": query, "cursor": cursor}}
            ) as resp:
                data = await resp.json()
                if "errors" in data or "message" in data:
                    print(f"API Error: {data.get('message', data.get('errors', 'Unknown'))}")
                    break
                nodes = data["data"]["search"]["nodes"]
                if not nodes:
                    break
                for node in nodes:
                    if node["id"] not in seen_repos:
                        seen_repos.add(node["id"])
                        repos.append(Repository(node["id"], node["nameWithOwner"], node.get("stargazerCount", 0)))
                if not data["data"]["search"]["pageInfo"]["hasNextPage"]:
                    break
                cursor = data["data"]["search"]["pageInfo"]["endCursor"]
        return repos

async def crawl_100k_repositories() -> List[Repository]:
    if not os.getenv("GITHUB_TOKEN"):
        print("❌ GITHUB_TOKEN required")
        sys.exit(1)
    seen_repos = set()
    queries = [
        "stars:>1000", "stars:100..999", "stars:10..99", "language:python stars:1..9",
        "language:javascript stars:1..9", "language:java stars:1..9", "created:2024-01-01..2025-09-21"
    ] * 25  # Increased repetition for 100,000
    all_repos = []
    semaphore = asyncio.Semaphore(10)
    
    async def fetch_with_limit(q):
        async with semaphore:
            return await fetch_repos(q, seen_repos)
    
    tasks = [fetch_with_limit(q) for q in queries]
    for batch in [tasks[i:i + 30] for i in range(0, len(tasks), 30)]:
        all_repos.extend([r for task in await asyncio.gather(*batch) for r in task])
        if len(all_repos) >= 100000:
            break
    return all_repos[:100000]

async def save_repos(repos: List[Repository]):
    conn = await asyncpg.connect(os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/github_crawler"))
    for i in range(0, len(repos), 2000):
        batch = repos[i:i + 2000]
        await conn.executemany(
            """
            INSERT INTO repositories (github_id, name_with_owner, star_count)
            VALUES ($1, $2, $3)
            ON CONFLICT (github_id) DO NOTHING
            """,
            [(r.github_id, r.name_with_owner, r.star_count) for r in batch]
        )
    await conn.close()

async def main():
    repos = await crawl_100k_repositories()
    print(f"🏁 Collected {len(repos)} repositories")
    await save_repos(repos)
    print("🎉 Done")

if __name__ == "__main__":
    asyncio.run(main())
