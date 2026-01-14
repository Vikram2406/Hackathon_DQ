#!/bin/bash

# Airflow Setup Script for Data Quality Platform
# This script sets up Apache Airflow locally (without Docker)

set -e

echo "🚀 Setting up Apache Airflow for Data Quality Platform"
echo "========================================================="
echo ""

# Get project root
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

# Check Python version
echo "1️⃣ Checking Python version..."
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "✅ Python version: $python_version"
echo ""

# Create Airflow directory
echo "2️⃣ Setting up Airflow directory..."
AIRFLOW_HOME="$PROJECT_ROOT/airflow_home"
export AIRFLOW_HOME

mkdir -p "$AIRFLOW_HOME"
echo "export AIRFLOW_HOME=$AIRFLOW_HOME" >> ~/.zshrc
echo "✅ Airflow home: $AIRFLOW_HOME"
echo ""

# Create virtual environment for Airflow
echo "3️⃣ Creating Airflow virtual environment..."
if [ ! -d "airflow_venv" ]; then
    python3 -m venv airflow_venv
    echo "✅ Virtual environment created"
else
    echo "✅ Virtual environment already exists"
fi
echo ""

# Activate virtual environment
echo "4️⃣ Activating virtual environment..."
source airflow_venv/bin/activate
echo "✅ Virtual environment activated"
echo ""

# Upgrade pip
echo "5️⃣ Upgrading pip..."
pip install --upgrade pip -q
echo "✅ Pip upgraded"
echo ""

# Install Airflow
echo "6️⃣ Installing Apache Airflow..."
echo "   This may take a few minutes..."

# Set Airflow version and Python version
AIRFLOW_VERSION=2.8.1
PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Install Airflow with constraints
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" -q

echo "✅ Airflow installed"
echo ""

# Initialize Airflow database
echo "7️⃣ Initializing Airflow database..."
airflow db init
echo "✅ Database initialized"
echo ""

# Create admin user
echo "8️⃣ Creating Airflow admin user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "✅ Admin user created (username: admin, password: admin)"
echo ""

# Configure Airflow
echo "9️⃣ Configuring Airflow..."

# Update airflow.cfg to point to project DAGs
cat >> "$AIRFLOW_HOME/airflow.cfg" << EOF

# Custom configuration for DQ Platform
dags_folder = $PROJECT_ROOT/airflow/dags
load_examples = False
EOF

echo "✅ Configuration updated"
echo ""

# Copy environment variables
echo "🔟 Setting up environment variables..."
if [ -f ".env" ]; then
    # Source .env file for Airflow
    cp .env "$AIRFLOW_HOME/.env"
    echo "✅ Environment variables copied"
else
    echo "⚠️  No .env file found. Please create one with your credentials."
fi
echo ""

# Create startup scripts
echo "📝 Creating startup scripts..."

# Script to start webserver
cat > start_airflow_webserver.sh << 'SCRIPT'
#!/bin/bash
cd "$(dirname "$0")"
export AIRFLOW_HOME="$(pwd)/airflow_home"
source airflow_venv/bin/activate
source .env
echo "🌐 Starting Airflow Webserver..."
echo "   Access Airflow UI at: http://localhost:8080"
echo "   Username: admin"
echo "   Password: admin"
echo ""
airflow webserver --port 8080
SCRIPT

chmod +x start_airflow_webserver.sh

# Script to start scheduler
cat > start_airflow_scheduler.sh << 'SCRIPT'
#!/bin/bash
cd "$(dirname "$0")"
export AIRFLOW_HOME="$(pwd)/airflow_home"
source airflow_venv/bin/activate
source .env
echo "⏰ Starting Airflow Scheduler..."
airflow scheduler
SCRIPT

chmod +x start_airflow_scheduler.sh

# Script to trigger DAG manually
cat > trigger_dag.sh << 'SCRIPT'
#!/bin/bash
cd "$(dirname "$0")"
export AIRFLOW_HOME="$(pwd)/airflow_home"
source airflow_venv/bin/activate
source .env

DAG_ID="dq_validation_s3"

echo "🚀 Triggering DAG: $DAG_ID"
airflow dags trigger $DAG_ID

echo ""
echo "✅ DAG triggered successfully!"
echo "   View progress at: http://localhost:8080"
SCRIPT

chmod +x trigger_dag.sh

echo "✅ Startup scripts created"
echo ""

# Summary
echo "========================================================="
echo "✅ Airflow Setup Complete!"
echo "========================================================="
echo ""
echo "📋 Next Steps:"
echo ""
echo "1. Start the Airflow Webserver (in one terminal):"
echo "   ./start_airflow_webserver.sh"
echo ""
echo "2. Start the Airflow Scheduler (in another terminal):"
echo "   ./start_airflow_scheduler.sh"
echo ""
echo "3. Access Airflow UI:"
echo "   URL: http://localhost:8080"
echo "   Username: admin"
echo "   Password: admin"
echo ""
echo "4. Trigger the DAG manually:"
echo "   ./trigger_dag.sh"
echo "   OR use the Airflow UI"
echo ""
echo "🎉 Your DAG 'dq_validation_s3' should appear in the Airflow UI!"
echo ""
