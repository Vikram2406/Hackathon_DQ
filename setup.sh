#!/bin/bash

# Data Quality Platform - Setup Script
# This script automates the setup process

set -e  # Exit on error

echo "🚀 Data Quality Platform - Setup Script"
echo "========================================"
echo ""

# Check if Docker is running
echo "1️⃣ Checking Docker..."
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop and try again."
    exit 1
fi
echo "✅ Docker is running"
echo ""

# Check if .env exists
echo "2️⃣ Checking environment configuration..."
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    cp .env.example .env
    echo "⚠️  IMPORTANT: Edit .env file and add your OPENAI_API_KEY"
    echo "   Get your API key from: https://platform.openai.com/api-keys"
    echo ""
    read -p "Press Enter after you've added your OpenAI API key to .env..."
fi
echo "✅ Environment file exists"
echo ""

# Start Docker services
echo "3️⃣ Starting database services..."
docker-compose down > /dev/null 2>&1 || true
docker-compose up -d
echo "✅ PostgreSQL and Redis started"
echo ""

# Wait for PostgreSQL to be ready
echo "4️⃣ Waiting for PostgreSQL to be ready..."
sleep 5
until docker-compose exec -T postgres pg_isready -U dq_user > /dev/null 2>&1; do
    echo "   Waiting for PostgreSQL..."
    sleep 2
done
echo "✅ PostgreSQL is ready"
echo ""

# Setup backend
echo "5️⃣ Setting up backend..."
cd backend

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "   Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment and install dependencies
echo "   Installing dependencies..."
source venv/bin/activate
pip install -q --upgrade pip
pip install -q -r requirements.txt

# Initialize database
echo "   Initializing database..."
python -c "from database import init_db; init_db()"

cd ..
echo "✅ Backend setup complete"
echo ""

# Setup frontend
echo "6️⃣ Setting up frontend..."
cd frontend

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "   Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment and install dependencies
echo "   Installing dependencies..."
source venv/bin/activate
pip install -q --upgrade pip
pip install -q -r requirements.txt

cd ..
echo "✅ Frontend setup complete"
echo ""

# Create sample data
echo "7️⃣ Checking sample data..."
if [ -f "sample_data/orders.csv" ]; then
    echo "✅ Sample data exists"
else
    echo "⚠️  Sample data not found. Please check sample_data/orders.csv"
fi
echo ""

# Summary
echo "========================================" 
echo "✅ Setup Complete!"
echo "========================================"
echo ""
echo "📋 Next Steps:"
echo ""
echo "1. Start the backend (in one terminal):"
echo "   cd backend"
echo "   source venv/bin/activate"
echo "   python main.py"
echo ""
echo "2. Start the frontend (in another terminal):"
echo "   cd frontend"
echo "   source venv/bin/activate"
echo "   streamlit run app.py"
echo ""
echo "3. Open your browser:"
echo "   Frontend: http://localhost:8501"
echo "   Backend API: http://localhost:8000/docs"
echo ""
echo "📖 For detailed instructions, see QUICKSTART.md"
echo ""
echo "🎉 Happy validating!"
