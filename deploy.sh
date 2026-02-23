#!/bin/bash
set -e

echo "🚀 Deploying ETF API..."

# Create required directories
mkdir -p data logs

# Check if required files exist
echo "📋 Checking required files..."
required_files=("docker-compose.yml" "Dockerfile" "requirements.txt" "phase1_production_api.py" "top_100_etfs_data.py" "nginx.conf")

for file in "${required_files[@]}"; do
    if [ ! -f "$file" ]; then
        echo "❌ Missing file: $file"
        exit 1
    else
        echo "✅ Found: $file"
    fi
done

# Stop any existing containers
echo "🛑 Stopping existing containers..."
docker-compose down 2>/dev/null || true

# Build and start services
echo "🔨 Building and starting services..."
docker-compose up -d --build

# Wait for services to start
echo "⏳ Waiting for services to start..."
sleep 30

# Health check
echo "🔍 Running health check..."
for i in {1..10}; do
    if curl -f http://localhost:8080/health >/dev/null 2>&1; then
        echo "✅ API is healthy!"
        break
    elif curl -f http://localhost/health >/dev/null 2>&1; then
        echo "✅ API is healthy via nginx!"
        break
    else
        echo "   Health check attempt $i/10..."
        sleep 10
    fi
done

echo ""
echo "🎉 Deployment completed!"
echo ""
echo "🔗 Access your API:"
echo "   Direct API: http://localhost:8080/docs"
echo "   Via Nginx: http://localhost/docs"
echo "   Health: http://localhost/health"
echo ""
echo "📊 Management:"
echo "   View logs: docker-compose logs -f"
echo "   Stop: docker-compose down"
echo "   Restart: docker-compose restart"