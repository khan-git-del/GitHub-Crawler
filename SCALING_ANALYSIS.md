# GitHub Crawler Scaling Analysis

## 1. Scaling to 500 Million Repositories

### Current Limitations
- **API Rate Limits**: Single token = 5,000 requests/hour → only 5M repos/hour theoretical max
- **Search API**: Maximum 1,000 results per query pattern
- **Infrastructure**: Single database can't handle 500M rows efficiently

### Required Changes

**API Strategy:**
- Use 100+ GitHub tokens across different IPs
- Switch from Search API to Repository enumeration by ID ranges
- Deploy workers across multiple cloud regions

**Infrastructure:**
- Kubernetes cluster with 50-100 crawler pods  
- Database sharding across 20-50 PostgreSQL instances
- Redis message queue for work coordination

**Database Sharding:**
```sql
-- Split by repository ID hash
CREATE TABLE repositories_shard_00 (LIKE repositories);
-- ... up to 50 shards
```

**Timeline & Cost:**
- Development: 3-6 months
- Monthly infrastructure: $25,000-$40,000

---

## 2. Schema Evolution for Extended Metadata

### Normalized Design
Keep core `repositories` table stable, add child tables for new data:

```sql
-- Issues tracking
CREATE TABLE issues (
    id SERIAL PRIMARY KEY,
    repo_id INTEGER REFERENCES repositories(id),
    github_issue_id VARCHAR(50) UNIQUE,
    comment_count INTEGER DEFAULT 0,
    created_at TIMESTAMP
);

-- Pull requests
CREATE TABLE pull_requests (
    id SERIAL PRIMARY KEY,
    repo_id INTEGER REFERENCES repositories(id),  
    github_pr_id VARCHAR(50) UNIQUE,
    review_count INTEGER DEFAULT 0,
    comment_count INTEGER DEFAULT 0
);

-- Unified comments
CREATE TABLE comments (
    id SERIAL PRIMARY KEY,
    github_comment_id VARCHAR(50) UNIQUE,
    issue_id INTEGER REFERENCES issues(id),
    pr_id INTEGER REFERENCES pull_requests(id),
    author VARCHAR(100),
    created_at TIMESTAMP,
    CHECK ((issue_id IS NULL) != (pr_id IS NULL))
);

-- Flexible CI data
CREATE TABLE ci_checks (
    id SERIAL PRIMARY KEY,
    pr_id INTEGER REFERENCES pull_requests(id),
    details JSONB  -- Flexible for different CI systems
);
```

### Efficient Updates
**Example: PR gets 10 new comments**
```sql
-- 1. Insert 10 new comment rows
INSERT INTO comments (pr_id, github_comment_id, author, created_at) VALUES (...);

-- 2. Update PR comment count (1 row affected)
UPDATE pull_requests SET comment_count = comment_count + 10 WHERE id = 123;
```

**Benefits:**
- Adding comments only affects comment table + 1 PR row
- No changes to main repositories table
- Historical data preserved
- Scales to millions of comments per repository

### Performance Optimization
- **Partitioning**: Split large tables by date ranges
- **Indexing**: Strategic indexes on foreign keys and timestamps  
- **JSONB**: Flexible CI metadata without schema changes
