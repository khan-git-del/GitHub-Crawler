#!/usr/bin/env python3
"""
GitHub Crawler - Crawls 100K repositories using multi-query strategy
Handles GitHub API 1K search limit with smart query segmentation
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
    updated_at: str

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
        """Generate search queries to bypass 1K limit"""
        queries = []
        
        # High-value repos by star count
        star_ranges = [
            ">=10000", "5000..9999", "2500..4999", "1000..2499", 
            "500..999", "250..499", "100..249", "50..99", "25..49"
        ]
        for stars in star_ranges:
            queries.append(f"stars:{stars}")
        
        # Popular languages
        languages = ["javascript", "python", "java", "typescript", "go", "rust", "c++", "php"]
        for lang in languages:
            for stars in ["10..24", "5..9", "2..4", "1"]:
                queries.append(f"language:{lang} stars:{stars}")
        
        # Recent repos for diversity  
        for month in range(1, 13):
            queries.append(f"stars:1..2 created:2023-{month:02d}-01..2023-{month:02d}-28")
        
        return queries[:100]  # Limit queries to avoid rate issues
    
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
                        updatedAt
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
        
        # Fetch up to 1000 repos per query (API limit)
        while len(repos) < 1000:
            try:
                async with self.session.post(
                    "https://api.github.com/graphql",
                    json={
                        "query": graphql_query,
                        "variables": {"searchQuery": query, "cursor": cursor}
                    }
                ) as response:
                    data = await response.json()
                
                if "errors" in data:
                    break
                
                search = data["data"]["search"]
                nodes = search["nodes"]
                
                if not nodes:
                    break
                
                # Add unique repos only
                for node in nodes:
                    if node["id"] not in self.seen_repos:
                        self.seen_repos.add(node["id"])
                        repos.append(Repository(
                            github_id=node["id"],
                            name_with_owner=node["nameWithOwner"],
                            star_count=node["stargazerCount"],
                            updated_at=node["updatedAt"]
                        ))
                
                # Check pagination
                page_info = search["pageInfo"]
                if not page_info["hasNextPage"]:
                    break
                cursor = page_info["endCursor"]
                
                # Rate limit check
                if data["data"]["rateLimit"]["remaining"] < 100:
                    await asyncio.sleep(60)  # Wait 1 minute
                
                await asyncio.sleep(0.1)  # Small delay
                
            except Exception as e:
                print(f"Error in query '{query}': {e}")
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
            
            if len(all_repos) % 10000 == 0:
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
            updated_at TIMESTAMP,
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
        INSERT INTO repositories (github_id, name_with_owner, star_count, updated_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (github_id) DO UPDATE SET
            star_count = EXCLUDED.star_count,
            updated_at = EXCLUDED.updated_at,
            crawled_at = CURRENT_TIMESTAMP
        """
        
        async with self.pool.acquire() as conn:
            await conn.executemany(sql, [
                (r.github_id, r.name_with_owner, r.star_count, r.updated_at)
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
        print("Get token from: https://github.com/settings/tokens")
        sys.exit(1)
    
    print("🚀 Starting GitHub Repository Crawler")
    start_time = datetime.now()
    
    try:
        async with Database(database_url) as db:
            await db.setup_tables()
            
            async with GitHubCrawler(github_token) as crawler:
                repositories = await crawler.crawl_100k()
                
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
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
