#!/usr/bin/env python3
"""
Database setup script for GitHub Crawler
"""

import asyncio
import asyncpg
import os
import sys

class DatabaseSetup:
    def __init__(self, connection_url: str):
        self.connection_url = connection_url
    
    async def setup_tables(self):
        """Create optimized database schema with comments for evolution"""
        sql = """
        -- Core table for repositories. For evolution, this remains stable; additional metadata in normalized child tables.
        DROP TABLE IF EXISTS repositories CASCADE;
        
        CREATE TABLE repositories (
            id SERIAL PRIMARY KEY,
            github_id VARCHAR(150) UNIQUE NOT NULL,
            name_with_owner VARCHAR(300) NOT NULL,
            star_count INTEGER NOT NULL DEFAULT 0,
            crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Indexes for performance
        CREATE INDEX IF NOT EXISTS idx_repositories_github_id ON repositories(github_id);
        CREATE INDEX IF NOT EXISTS idx_repositories_stars ON repositories(star_count DESC);
        CREATE INDEX IF NOT EXISTS idx_repositories_name ON repositories(name_with_owner);
        
        -- For future metadata (e.g., issues, PRs, comments):
        -- Add tables like:
        -- CREATE TABLE issues (id SERIAL PK, repo_id INT FK REFERENCES repositories(id), issue_id VARCHAR UNIQUE, ...);
        -- CREATE TABLE prs (id SERIAL PK, repo_id INT FK, pr_id VARCHAR UNIQUE, ...);
        -- CREATE TABLE comments (id SERIAL PK, pr_id INT FK or issue_id INT FK, comment_id VARCHAR UNIQUE, ...);
        -- Updates: Insert new rows in child tables (e.g., new comments) without affecting parent rows.
        -- Use JSONB for flexible fields like CI checks to minimize schema changes.
        """
        
        pool = await asyncpg.create_pool(self.connection_url, min_size=1, max_size=1)
        async with pool.acquire() as conn:
            await conn.execute(sql)
        await pool.close()
        
        print("✅ Database schema created with optimized indexes and evolution notes")

async def main():
    database_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/github_crawler")
    
    setup = DatabaseSetup(database_url)
    await setup.setup_tables()

if __name__ == "__main__":
    asyncio.run(main())
