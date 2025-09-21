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
from dataclasses import dataclass
from typing import List, Set, Optional
from datetime import datetime
import json
import random

@dataclass
class Repository:
    github_id: str
    name_with_owner: str
    star_count: int

class ProductionGitHubCrawler:
    def __init__(self, token: str):
        self.token = token
        self.session = None
        self.seen_repos: Set[str] = set()
        self.rate_limit_remaining = 5000
        self.rate_limit_reset_at = None
        
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
    
    async def check_rate_limit(self):
        """Proactively check and wait for rate limits"""
        if self.rate_limit_remaining < 100:
            wait_time = 300  # Wait 5 minutes
            print(f"⏳ Rate limit low ({self.rate_limit_remaining}), waiting {wait_time}s...")
            await asyncio.sleep(wait_time)
            self.rate_limit_remaining = 5000  # Reset assumption
    
    def generate_comprehensive_queries(self) -> List[str]:
        """Generate 300+ queries to ensure 100K coverage"""
        queries = []
        
        # 1. Star-based queries (high value repos first)
        star_ranges = [
            ">=100000", "50000..99999", "25000..49999", "10000..24999", 
            "5000..9999", "2500..4999", "1500..2499", "1000..1499",
            "800..999", "600..799", "500..599", "400..499", "350..399",
            "300..349", "250..299", "200..249", "175..199", "150..174",
            "125..149", "100..124", "90..99", "80..89", "70..79", 
            "60..69", "50..59", "45..49", "40..44", "35..39", "30..34",
            "25..29", "22..24", "20..21", "18..19", "16..17", "14..15",
            "12..13", "10..11", "8..9", "6..7", "4..5", "3", "2", "1"
        ]
        
        for star_range in star_ranges:
            queries.append(f"stars:{star_range}")
        
        # 2. Language + star combinations (most comprehensive)
        languages = [
            "javascript", "python", "java", "typescript", "c++", "c", "php", 
            "c#", "go", "rust", "swift", "kotlin", "ruby", "scala", "dart",
            "r", "shell", "css", "html", "vue", "objective-c", "perl", 
            "matlab", "assembly", "powershell", "haskell", "lua", "groovy",
            "elixir", "clojure", "erlang", "f#", "julia", "nim", "crystal",
            "zig", "v", "solidity", "move", "cairo"
        ]
        
        # Multiple star ranges per language
        lang_star_ranges = [
            "100..999", "50..99", "25..49", "15..24", "10..14", "8..9", 
            "6..7", "5", "4", "3", "2", "1"
        ]
        
        for lang in languages:
            for star_range in lang_star_ranges:
                queries.append(f"language:{lang} stars:{star_range}")
        
        # 3. Topic-based queries for diversity
        topics = [
            "web", "api", "framework", "library", "tool", "bot", "game", 
            "mobile", "desktop", "cli", "database", "machine-learning",
            "ai", "blockchain", "security", "testing", "documentation",
            "frontend", "backend", "fullstack", "devops", "microservice",
            "react", "vue", "angular", "node", "express", "django", "flask",
            "spring", "laravel", "rails", "nextjs", "gatsby", "nuxt"
        ]
        
        for topic in topics:
            for stars in ["10..50", "5..9", "3..4", "1..2"]:
                queries.append(f"topic:{topic} stars:{stars}")
        
        # 4. Time-based queries for recent repos
        years = [2020, 2021, 2022, 2023, 2024, 2025]
        for year in years:
            months = 12 if year < 2025 else 9  # Only up to September 2025
            for month in range(1, months + 1):
                for stars in ["1", "2", "3..5"]:
                    queries.append(f"stars:{stars} created:{year}-{month:02d}-01..{year}-{month:02d}-28")
        
        # 5. Size-based queries (smaller repos often have 1-2 stars)
        sizes = ["<1000", "1000..5000", "5000..10000", "10000..50000", ">50000"]
        for size in sizes:
            queries.append(f"size:{size} stars:1..3")
        
        # 6. License-based queries
        licenses = ["mit", "apache-2.0", "gpl-3.0", "bsd-3-clause", "unlicense"]
        for license_type in licenses:
            for stars in ["1", "2", "3..4"]:
                queries.append(f"license:{license_type} stars:{stars}")
        
        # 7. Recently updated repos (active projects)
        for stars in ["1", "2", "3", "4", "5..10"]:
            queries.append(f"stars:{stars} pushed:>2024-01-01")
        
        print(f"🎯 Generated {len(queries)} comprehensive search queries")
        return queries
    
    async def execute_graphql_query(self, search_query: str) -> List[Repository]:
        """Execute a GraphQL search with robust error handling"""
        graphql_query = """
        query($searchQuery: String!, $cursor: String) {
            search(query: $searchQuery, type: REPOSITORY, first: 100, after: $cursor) {
                repositoryCount
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
                resetAt
            }
        }
        """
        
        repositories = []
        cursor = None
        max_pages = 10  # Max 1000 repos per query
        pages_fetched = 0
        
        while pages_fetched < max_pages:
            await self.check_rate_limit()
            
            try:
                async with self.session.post(
                    "https://api.github.com/graphql",
                    json={
                        "query": graphql_query,
                        "variables": {
                            "searchQuery": search_query,
                            "cursor": cursor
                        }
                    }
                ) as response:
                    
                    # Handle rate limiting
                    if response.status == 403:
                        print(f"🚫 Rate limited on query: {search_query}")
                        await asyncio.sleep(300)  # Wait 5 minutes
                        continue
                    
                    if response.status != 200:
                        print(f"❌ HTTP {response.status} for query: {search_query}")
                        break
                    
                    data = await response.json()
                    
                    # Handle GraphQL errors
                    if "errors" in data:
                        error_msg = data["errors"][0].get("message", "Unknown error")
                        if "timeout" in error_msg.lower():
                            print(f"⏰ Query timeout: {search_query}")
                            break
                        else:
                            print(f"🔴 GraphQL error in '{search_query}': {error_msg}")
                            break
                    
                    if "data" not in data or not data["data"]:
                        print(f"📭 No data for query: {search_query}")
                        break
                    
                    # Update rate limit info
                    rate_limit = data["data"].get("rateLimit", {})
                    self.rate_limit_remaining = rate_limit.get("remaining", 0)
                    
                    search_result = data["data"]["search"]
                    nodes = search_result.get("nodes", [])
                    
                    if not nodes:
                        break
                    
                    # Process repositories
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
                    
                    # Check pagination
                    page_info = search_result.get("pageInfo", {})
                    if not page_info.get("hasNextPage", False):
                        break
                    cursor = page_info.get("endCursor")
                    
                    # Dynamic delay based on rate limit
                    delay = 0.5 if self.rate_limit_remaining > 1000 else 2.0
                    await asyncio.sleep(delay)
                    
            except asyncio.TimeoutError:
                print(f"⏰ Timeout on query: {search_query}")
                break
            except Exception as e:
                print(f"💥 Network error in query '{search_query}': {str(e)[:100]}")
                await asyncio.sleep(5)
                break
        
        return repositories
    
    async def crawl_100k_repositories(self) -> List[Repository]:
        """Main method to collect exactly 100,000 repositories"""
        all_repositories = []
        queries = self.generate_comprehensive_queries()
        
        # Randomize query order to avoid patterns
        random.shuffle(queries)
        
        print(f"🚀 Starting comprehensive crawl with {len(queries)} queries")
        print(f"🎯 Target: 100,000 repositories")
        
        for query_idx, query in enumerate(queries):
            if len(all_repositories) >= 100000:
                break
            
            print(f"\n📊 Query {query_idx + 1}/{len(queries)}: {query}")
            print(f"📈 Progress: {len(all_repositories):,}/100,000 repos ({len(all_repositories)/1000:.1f}%)")
            
            batch_repos = await self.execute_graphql_query(query)
            all_repositories.extend(batch_repos)
            
            print(f"✅ Added {len(batch_repos)} repos | Total: {len(all_repositories):,}")
            
            # Progress milestones
            if len(all_repositories) % 10000 == 0 and len(all_repositories) > 0:
                print(f"🎉 MILESTONE: {len(all_repositories):,} repositories collected!")
            
            # Save progress periodically
            if query_idx % 50 == 0 and query_idx > 0:
                print(f"💾 Checkpoint: {len(all_repositories):,} repos after {query_idx} queries")
        
        final_repos = all_repositories[:100000]  # Ensure exactly 100K
        print(f"\n🏁 CRAWL COMPLETE: {len(final_repos):,} repositories collected")
        return final_repos

class Database:
    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self.pool = None
    
    async def __aenter__(self):
        self.pool = await asyncpg.create_pool(self.connection_url, min_size=1, max_size=3)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.pool:
            await self.pool.close()
    
    async def setup_tables(self):
        """Create optimized database schema"""
        sql = """
        DROP TABLE IF EXISTS repositories CASCADE;
        
        CREATE TABLE repositories (
            id SERIAL PRIMARY KEY,
            github_id VARCHAR(150) UNIQUE NOT NULL,
            name_with_owner VARCHAR(300) NOT NULL,
            star_count INTEGER NOT NULL DEFAULT 0,
            crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_repositories_github_id ON repositories(github_id);
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_repositories_stars ON repositories(star_count DESC);
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_repositories_name ON repositories(name_with_owner);
        """
        
        async with self.pool.acquire() as conn:
            # Drop indexes first, then create table, then create indexes
            await conn.execute("DROP INDEX IF EXISTS idx_repositories_github_id CASCADE;")
            await conn.execute("DROP INDEX IF EXISTS idx_repositories_stars CASCADE;")
            await conn.execute("DROP INDEX IF EXISTS idx_repositories_name CASCADE;")
            await conn.execute(sql.split("CREATE INDEX")[0])  # Create table only
            
            # Create indexes separately
            await conn.execute("CREATE INDEX idx_repositories_github_id ON repositories(github_id);")
            await conn.execute("CREATE INDEX idx_repositories_stars ON repositories(star_count DESC);")
            await conn.execute("CREATE INDEX idx_repositories_name ON repositories(name_with_owner);")
            
        print("✅ Database schema created with optimized indexes")
    
    async def save_repositories_batch(self, repositories: List[Repository]):
        """Optimized batch insert"""
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
    
    async def get_comprehensive_stats(self) -> dict:
        """Get detailed crawl statistics"""
        async with self.pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM repositories")
            
            top_10 = await conn.fetch("""
                SELECT name_with_owner, star_count 
                FROM repositories 
                ORDER BY star_count DESC 
                LIMIT 10
            """)
            
            star_distribution = await conn.fetch("""
                SELECT 
                    CASE 
                        WHEN star_count >= 10000 THEN '10K+'
                        WHEN star_count >= 1000 THEN '1K-10K'
                        WHEN star_count >= 100 THEN '100-1K'
                        WHEN star_count >= 10 THEN '10-100'
                        ELSE '1-9'
                    END as star_range,
                    COUNT(*) as count
                FROM repositories 
                GROUP BY 1 
                ORDER BY MIN(star_count) DESC
            """)
            
            return {
                "total_repositories": total,
                "top_starred": [dict(row) for row in top_10],
                "star_distribution": [dict(row) for row in star_distribution]
            }

async def main():
    # Environment setup
    github_token = os.getenv("GITHUB_TOKEN")
    database_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/github_crawler")
    
    if not github_token:
        print("❌ ERROR: GITHUB_TOKEN environment variable required")
        sys.exit(1)
    
    print("🚀 GitHub Production Crawler - Target: 100,000 repositories")
    start_time = datetime.now()
    
    try:
        async with Database(database_url) as db:
            print("📊 Setting up database schema...")
            await db.setup_tables()
            
            async with ProductionGitHubCrawler(github_token) as crawler:
                repositories = await crawler.crawl_100k_repositories()
                
                if not repositories:
                    print("❌ No repositories collected - check API access")
                    sys.exit(1)
                
                print(f"\n💾 Saving {len(repositories):,} repositories to database...")
                
                # Save in optimized batches
                batch_size = 2000
                for i in range(0, len(repositories), batch_size):
                    batch = repositories[i:i + batch_size]
                    await db.save_repositories_batch(batch)
                    print(f"💾 Saved batch {i//batch_size + 1}: {len(batch)} repos")
                
                # Final statistics
                stats = await db.get_comprehensive_stats()
                duration = (datetime.now() - start_time).total_seconds()
                
                print(f"\n🎉 MISSION ACCOMPLISHED!")
                print(f"⏱️  Total Duration: {duration/60:.1f} minutes")
                print(f"📊 Final Count: {stats['total_repositories']:,} repositories")
                print(f"⭐ Top Repository: {stats['top_starred'][0]['name_with_owner']} ({stats['top_starred'][0]['star_count']:,} stars)")
                
                print(f"\n📈 Star Distribution:")
                for dist in stats['star_distribution']:
                    print(f"   {dist['star_range']}: {dist['count']:,} repos")
    
    except Exception as e:
        print(f"❌ Critical Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
