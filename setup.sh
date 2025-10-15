#!/bin/bash
set -e

echo "============================================"
echo " 🚀 ETL Pipeline Setup (Airflow + MSSQL + CloudBeaver)"
echo "============================================"

# Navigate to docker folder
cd docker

# Copy environment template if missing
if [ ! -f ".env" ]; then
    echo "⚙️  Creating .env from example..."
    cp ../.env.example .env
fi

echo "🐳 Building Docker images..."
docker compose build

echo "🔌 Starting containers..."
docker compose up -d

echo "✅ All containers are up!"
echo "--------------------------------------------"
echo "🌐 Airflow Web UI:     http://localhost:8080"
echo "📦 CloudBeaver UI:     http://localhost:8978"
echo "🗄️  MSSQL Server Port: 1433"
echo "--------------------------------------------"
