#!/usr/bin/env python3
"""
Production GitHub Crawler - Designed to collect exactly 100,000 repositories
Uses intelligent rate limiting and multiple strategies
"""

import asyncio
import aiohttp
import asyncpg
import os
import sys
from dataclasses import dataclass, frozen=True
from typing import List, Set, Optional, Tuple
from datetime import datetime
import json
import random
from tenacity import retry, stop_after_attempt, wait_exponential

@dataclass(frozen=True)
class Repository:
    github_id: str
    name_with_owner: str
    star_count: int

class GitHubAPIAdapter:
    """Anti-corruption layer for GitHub API interactions"""
    def __init__(self, token: str):
        self.token = token
        self.session = None
        self.rate_limit_remaining: int = 5000
        self.rate_limit_reset_at: Optional[datetime] = None
        self.rate_limit_limit: int = 5000
        self.rate_limit_cost: int = 1  # Default assumption

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers={
                "Authorization": f"Bearer {self.token}",
                "User-Agent": "GitHub-Crawler/1.0"
            },
            timeout=aiohttp.ClientTimeout(total=30)
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def check_rate_limit(self, expected_cost: int = 1):
        """Proactively check and wait for rate limits"""
        if self.rate_limit_remaining < (expected_cost + 100):
            if self.rate_limit_reset_at:
                wait_time = max(0, (self.rate_limit_reset_at - datetime.utcnow()).total_seconds() + 10)
            else:
                wait_time = 60  # Default if unknown
            print(f"⏳ Rate limit low ({self.rate_limit_remaining}/{self.rate_limit_limit}), waiting {wait_time}s...")
            await asyncio.sleep(wait_time)

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=60))
    async def execute_graphql(self, query: str, variables: dict) -> dict:
        """Execute GraphQL with retries"""
        await self.check_rate_limit(expected_cost=1)  # Assume cost ~1-2
        
        async with self.session.post(
            "https://api.github.com/graphql",
            json={"query": query, "variables": variables}
        ) as response:
            if response.status == 403:
                retry_after = int(response.headers.get('Retry-After', 60))
                print(f"🚫 Rate limited (403), retrying after {retry_after}s")
                await asyncio.sleep(retry_after)
                raise Exception("Rate limited")  # Trigger retry
            
            if response.status != 200:
                raise Exception(f"HTTP {response.status}")
            
            data = await response.json()
            if "errors" in data:
                raise Exception(f"GraphQL error: {data['errors'][0]['message']}")
            
            rate_limit = data["data"].get("rateLimit", {})
            self.rate_limit_remaining = rate_limit.get("remaining", 0)
            self.rate_limit_reset_at = datetime.fromisoformat(rate_limit.get("resetAt").replace('Z', '+00:00')) if rate_limit.get("resetAt") else None
            self.rate_limit_limit = rate_limit.get("limit", 5000)
            self.rate_limit_cost = rate_limit.get("cost", 1)
            
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
        """Generate ~200 optimized queries for faster coverage"""
        queries = []
        
        # 1. Star-based queries (broader ranges)
        star_ranges = [
            ">=50000", "10000..49999", "5000..9999", "1000..4999",
            "500..999", "200..499", "100..199", "50..99", "20..49",
            "10..19", "5..9", "3..4", "2", "1"
        ]
        for star_range in star_ranges:
            queries.append(f"stars:{star_range} sort:stars-desc")

        # 2. Language + star combinations (reduced)
        languages = [
            "javascript", "python", "java", "typescript", "c++", "go", "rust",
            "php", "c#", "swift", "kotlin", "ruby"
        ]
        lang_star_ranges = ["50..*", "20..49", "10..19", "5..9", "1..4"]
        for lang in languages:
            for star_range in lang_star_ranges:
                queries.append(f"language:{lang} stars:{star_range}")

        # 3. Topic-based (reduced)
        topics = ["web", "api", "framework", "machine-learning", "ai", "blockchain", "security"]
        for topic in topics:
            queries.append(f"topic:{topic} stars:1..10")

        # 4. Time-based (quarterly instead of monthly)
        years = [2020, 2021, 2022, 2023, 2024, 2025]
        for year in years:
            quarters = 4 if year < 2025 else 3  # Up to Q3 2025
            for q in range(1, quarters + 1):
                start_month = (q-1)*3 + 1
                end_month = start_month + 2
                queries.append(f"stars:1..5 created:{year}-{start_month:02d}-01..{year}-{end_month:02d}-28")

        # 5. Other diverse queries (reduced)
        for size in ["<5000", "5000..50000", ">50000"]:
            queries.append(f"size:{size} stars:1..3")
        for license_type in ["mit", "apache-2.0", "gpl-3.0"]:
            queries.append(f"license:{license_type} stars:1..5")
        queries.append("stars:1..10 pushed:>2024-01-01")

        print(f"🎯 Generated {len(queries)} optimized search queries")
        return queries
    
    async def execute_graphql_query(self, search_query: str) -> List[Repository]:
        """Fetch repos for a query using adapter"""
        graphql_query = """
        query($searchQuery: String!, $cursor: String) {
            search(query: $searchQuery, type: REPOSITORY, first: 100, after: $cursor) {
                repositoryCount
                nodes {
                    ... on Repository {
                        id
                        nameWithOwner
                        stargazerCount
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
            rateLimit {
                limit
                cost
                remaining
                resetAt
            }
        }
        """
        
        repositories = []
        cursor = None
        max_pages = 10  # Max 1000 per query
        pages_fetched = 0
        
        while pages_fetched < max_pages:
            data = await self.api_adapter.execute_graphql(
                graphql_query,
                {"searchQuery": search_query, "cursor": cursor}
            )
            
            search_result = data["search"]
            nodes = search_result.get("nodes", [])
            
            if not nodes:
                break
            
            batch_added = 0
            for node in nodes:
                if not node or "id" not in node:
                    continue
                repo_id = node["id"]
                if repo_id not in self.seen_repos:
                    self.seen_repos.add(repo_id)
                    repositories.append(Repository(
                        github_id=repo_id,
                        name_with_owner=node.get("nameWithOwner", "unknown/unknown"),
                        star_count=max(0, node.get("stargazerCount", 0))
                    ))
                    batch_added += 1
            
            pages_fetched += 1
            page_info = search_result.get("pageInfo", {})
            if not page_info.get("hasNextPage", False):
                break
            cursor = page_info.get("endCursor")
            
            # Dynamic delay
            delay = 0.1 if self.api_adapter.rate_limit_remaining > 2000 else 1.0
            await asyncio.sleep(delay)
        
        return repositories
    
    async def crawl_100k_repositories(self) -> Tuple[List[Repository]]:
        """Collect 100,000 repos with parallel batches"""
        all_repositories = []
        queries = self.generate_comprehensive_queries()
        random.shuffle(queries)
        
        print(f"🚀 Starting optimized crawl with {len(queries)} queries")
        print(f"🎯 Target: 100,000 repositories")
        
        semaphore = asyncio.Semaphore(5)  # Limit concurrent queries
        
        async def limited_exec(query):
            async with semaphore:
                return await self.execute_graphql_query(query)
        
        batch_size = 10
        for batch_start in range(0, len(queries), batch_size):
            if len(all_repositories) >= 100000:
                break
            
            batch_queries = queries[batch_start:batch_start + batch_size]
            print(f"\n📊 Processing batch {batch_start // batch_size + 1} ({len(batch_queries)} queries)")
            print(f"📈 Progress: {len(all_repositories):,}/100,000 ({len(all_repositories)/1000:.1f}%)")
            
            batch_results = await asyncio.gather(*[limited_exec(q) for q in batch_queries])
            for batch_repos in batch_results:
                all_repositories.extend(batch_repos)
            
            print(f"✅ Added {sum(len(b) for b in batch_results)} repos | Total: {len(all_repositories):,}")
            
            if len(all_repositories) % 10000 == 0 and len(all_repositories) > 0:
                print(f"🎉 MILESTONE: {len(all_repositories):,} repositories collected!")
        
        final_repos = tuple(all_repositories[:100000])  # Immutable tuple, exactly 100K
        print(f"\n🏁 CRAWL COMPLETE: {len(final_repos):,} repositories collected")
        return final_repos

class RepositorySaver:
    """Separated concern for saving repositories"""
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
            await conn.executemany(sql, [
                (r.github_id, r.name_with_owner, r.star_count)
                for r in repositories
            ])

async def main():
    github_token = os.getenv("GITHUB_TOKEN")
    database_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/github_crawler")
    
    if not github_token:
        print("❌ ERROR: GITHUB_TOKEN environment variable required")
        sys.exit(1)
    
    print("🚀 GitHub Production Crawler - Target: 100,000 repositories")
    start_time = datetime.now()
    
    try:
        async with ProductionGitHubCrawler(github_token) as crawler:
            repositories = await crawler.crawl_100k_repositories()
            
            if not repositories:
                print("❌ No repositories collected - check API access")
                sys.exit(1)
            
            print(f"\n💾 Saving {len(repositories):,} repositories to database...")
            
            async with RepositorySaver(database_url) as saver:
                batch_size = 2000
                for i in range(0, len(repositories), batch_size):
                    batch = repositories[i:i + batch_size]
                    await saver.save_repositories_batch(batch)
                    print(f"💾 Saved batch {i//batch_size + 1}: {len(batch)} repos")
            
            duration = (datetime.now() - start_time).total_seconds()
            print(f"\n🎉 MISSION ACCOMPLISHED!")
            print(f"⏱️  Total Duration: {duration/60:.1f} minutes")
    
    except Exception as e:
        print(f"❌ Critical Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
