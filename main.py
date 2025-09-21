#!/usr/bin/env python3
"""
GitHub Crawler - Fixed version that handles API errors properly
"""

import asyncio
import aiohttp
import asyncpg
import os
import sys
from dataclasses import dataclass
from typing import List, Set
from datetime import datetime

@dataclass
class Repository:
    github_id: str
    name_with_owner: str
    star_count: int

class GitHubCrawler:
    def __init__(self, token: str):
        self.token = token
        self.session = None
        self.seen_repos: Set[str] = set()
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {self.token}"}
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    def get_search_queries(self) -> List[str]:
        """Generate many more search queries to reach 100K"""
        queries = []
        
        # High-value repos (these work well)
        star_ranges = [
            ">=50000", "25000..49999", "10000..24999", "5000..9999", 
            "2500..4999", "1000..2499", "500..999", "250..499", 
            "100..249", "50..99", "25..49", "10..24", "5..9", "2..4", "1"
        ]
        for stars in star_ranges:
            queries.append(f"stars:{stars}")
        
        # All major languages with various star counts
        languages = [
            "javascript", "python", "java", "typescript", "c++", "c", "php", 
            "c#", "go", "rust", "swift", "kotlin", "ruby", "scala", "dart",
            "r", "shell", "css", "html", "vue", "objective-c", "perl", 
            "matlab", "assembly", "powershell", "haskell", "lua", "groovy"
        ]
        
        # More granular star ranges for languages
        lang_star_ranges = ["50..99", "25..49", "10..24", "5..9", "3..4", "2", "1"]
        for lang in languages:
            for stars in lang_star_ranges:
                queries.append(f"language:{lang} stars:{stars}")
        
        # Time-based queries for more coverage (recent repos)
        years = [2023, 2024, 2025]
        for year in years:
            for month in range(1, 13):
                if year == 2025 and month > 9:  # Don't query future months
                    continue
                queries.append(f"stars:1..3 created:{year}-{month:02d}-01..{year}-{month:02d}-28")
        
        # Topic-based queries for diversity
        topics = [
            "web", "api", "framework", "library", "tool", "bot", "game", 
            "mobile", "desktop", "cli", "database", "machine-learning",
            "ai", "blockchain", "security", "testing", "documentation"
        ]
        for topic in topics:
            for stars in ["1..2", "3..5"]:
                queries.append(f"topic:{topic} stars:{stars}")
        
        print(f"Generated {len(queries)} search queries for maximum coverage")
        return queries
    
    async def fetch_batch(self, query: str) -> List[Repository]:
        """Fetch repositories for one search query"""
        graphql_query = """
        query($searchQuery: String!, $cursor: String) {
            search(query: $searchQuery, type: REPOSITORY, first: 100, after: $cursor) {
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
                remaining
            }
        }
        """
        
        repos = []
        cursor = None
        
        # Fetch up to 1000 repos per query
        while len(repos) < 1000:
            try:
                async with self.session.post(
                    "https://api.github.com/graphql",
                    json={
                        "query": graphql_query,
                        "variables": {"searchQuery": query, "cursor": cursor}
                    }
                ) as response:
                    
                    if response.status == 403:
                        print(f"Rate limited on query: {query}, waiting 5 minutes...")
                        await asyncio.sleep(300)  # Wait 5 minutes on rate limit
                        continue  # Retry the same query
                    elif response.status != 200:
                        print(f"HTTP {response.status} for query: {query}")
                        break
                    
                    data = await response.json()
                
                    if "errors" in data:
                        print(f"GraphQL Error in '{query}': {data['errors'][0]['message']}")
                        break
                    
                    if "data" not in data or not data["data"]:
                        print(f"No data returned for query: {query}")
                        break
                    
                    search = data["data"]["search"]
                    if not search or "nodes" not in search:
                        print(f"Invalid search response for: {query}")
                        break
                        
                    nodes = search["nodes"]
                    
                    if not nodes:
                        break
                    
                    # Add unique repos only
                    for node in nodes:
                        if node and "id" in node and node["id"] not in self.seen_repos:
                            self.seen_repos.add(node["id"])
                            repos.append(Repository(
                                github_id=node["id"],
                                name_with_owner=node.get("nameWithOwner", "unknown/unknown"),
                                star_count=node.get("stargazerCount", 0)
                            ))
                    
                    # Check pagination
                    page_info = search.get("pageInfo", {})
                    if not page_info.get("hasNextPage", False):
                        break
                    cursor = page_info.get("endCursor")
                    
                    # Rate limit handling - be more aggressive about waiting
                    rate_limit = data["data"].get("rateLimit", {})
                    remaining = rate_limit.get("remaining", 1000)
                    if remaining < 500:  # Wait earlier to avoid 403s
                        wait_time = min(300, (1000 - remaining) * 0.5)  # Scale wait time
                        print(f"Rate limit at {remaining}, waiting {wait_time:.1f}s...")
                        await asyncio.sleep(wait_time)
                    
                    await asyncio.sleep(1.0)  # Longer delay between requests
                    
            except Exception as e:
                print(f"Network error in query '{query}': {e}")
                break
        
        return repos
    
    async def crawl_100k(self) -> List[Repository]:
        """Main crawling function"""
        all_repos = []
        queries = self.get_search_queries()
        
        print(f"Starting crawl with {len(queries)} search queries...")
        
        for i, query in enumerate(queries):
            if len(all_repos) >= 100000:
                break
                
            print(f"Query {i+1}: {query} | Total repos: {len(all_repos)}")
            
            repos = await self.fetch_batch(query)
            all_repos.extend(repos)
            
            if len(all_repos) % 5000 == 0 and len(all_repos) > 0:
                print(f"🎯 Milestone: {len(all_repos)} repos collected")
        
        return all_repos[:100000]

class Database:
    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self.pool = None
    
    async def __aenter__(self):
        self.pool = await asyncpg.create_pool(self.connection_url)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.pool:
            await self.pool.close()
    
    async def setup_tables(self):
        """Create database schema"""
        sql = """
        DROP TABLE IF EXISTS repositories CASCADE;
        
        CREATE TABLE repositories (
            id SERIAL PRIMARY KEY,
            github_id VARCHAR(100) UNIQUE NOT NULL,
            name_with_owner VARCHAR(200) NOT NULL,
            star_count INTEGER NOT NULL,
            crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX idx_github_id ON repositories(github_id);
        CREATE INDEX idx_star_count ON repositories(star_count DESC);
        """
        
        async with self.pool.acquire() as conn:
            await conn.execute(sql)
        print("✅ Database tables created")
    
    async def save_repos(self, repositories: List[Repository]):
        """Save repositories to database"""
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
        
        print(f"💾 Saved {len(repositories)} repositories")
    
    async def get_stats(self):
        """Get crawl statistics"""
        async with self.pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM repositories")
            top = await conn.fetchrow(
                "SELECT name_with_owner, star_count FROM repositories ORDER BY star_count DESC LIMIT 1"
            )
            return count, top

async def main():
    # Get environment variables
    github_token = os.getenv("GITHUB_TOKEN")
    database_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/github_crawler")
    
    if not github_token:
        print("❌ ERROR: GITHUB_TOKEN environment variable required")
        sys.exit(1)
    
    print("🚀 Starting GitHub Repository Crawler", flush=True)
    start_time = datetime.now()
    
    try:
        async with Database(database_url) as db:
            await db.setup_tables()
            
            async with GitHubCrawler(github_token) as crawler:
                repositories = await crawler.crawl_100k()
                
                if not repositories:
                    print("❌ No repositories were collected")
                    sys.exit(1)
                
                # Save in batches
                batch_size = 1000
                for i in range(0, len(repositories), batch_size):
                    batch = repositories[i:i + batch_size]
                    await db.save_repos(batch)
                
                # Show results
                count, top = await db.get_stats()
                duration = (datetime.now() - start_time).total_seconds()
                
                print(f"\n🎉 CRAWL COMPLETED!")
                print(f"⏱️  Duration: {duration:.1f} seconds")
                print(f"📊 Repositories: {count:,}")
                if top:
                    print(f"⭐ Top starred: {top['name_with_owner']} ({top['star_count']:,} stars)")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
