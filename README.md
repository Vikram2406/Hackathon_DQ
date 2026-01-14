# AI-Powered Data Quality Platform

Enterprise-grade data quality automation system with AI-powered insights and natural language interface.

## 🚀 Features

- **Multi-Source Support**: S3, Snowflake, CSV
- **Automated Quality Checks**: Null, Duplicate, Freshness, Volume Anomaly
- **AI-Powered Insights**: Root cause analysis and explanations
- **Natural Language Chatbot**: Ask questions in plain English
- **Production-Ready**: Built with Airflow, FastAPI, PostgreSQL

## 🏗️ Architecture

```
Frontend (Streamlit) → Backend (FastAPI) → Airflow → Data Quality Engine
                              ↓
                        PostgreSQL + Redis
                              ↓
                        AI Service (OpenAI)
```

## 📋 Prerequisites

- Docker & Docker Compose
- Python 3.9+
- OpenAI API Key
- AWS Credentials (for S3 support)

## 🛠️ Quick Start

### 1. Clone and Setup

```bash
git clone <repository-url>
cd Hackathon_26
cp .env.example .env
# Edit .env with your credentials
```

### 2. Start Services

```bash
docker-compose up -d
```

### 3. Access Applications

- **Frontend UI**: http://localhost:8501
- **Backend API**: http://localhost:8000
- **Airflow**: http://localhost:8080 (admin/admin)
- **API Docs**: http://localhost:8000/docs

### 4. Run Demo

1. Open Frontend UI
2. Configure a dataset (use sample data in `sample_data/`)
3. Run quality validation
4. View results and AI insights
5. Ask questions via chatbot

## 📁 Project Structure

```
Hackathon_26/
├── frontend/              # Streamlit UI
│   ├── app.py            # Main application
│   ├── pages/            # Multi-page app
│   └── requirements.txt
├── backend/              # FastAPI backend
│   ├── main.py          # API endpoints
│   ├── models/          # Database models
│   ├── services/        # Business logic
│   └── connectors/      # Data source connectors
├── airflow/             # Airflow DAGs
│   ├── dags/           # DAG definitions
│   └── plugins/        # Custom operators
├── dq_engine/          # Data Quality Engine
│   ├── checks/         # Quality check implementations
│   └── ai/             # AI-powered analysis
├── chatbot/            # Chatbot service
├── sample_data/        # Demo datasets
└── docker-compose.yml  # Service orchestration
```

## 🧪 Running Tests

```bash
# Backend tests
cd backend
pytest

# Integration tests
pytest tests/integration/
```

## 📊 Sample Datasets

Sample datasets with injected quality issues are provided in `sample_data/`:

- `orders.csv` - E-commerce orders (null values, duplicates, volume anomaly)
- `user_activity.csv` - User activity logs (freshness issues)
- `transactions.csv` - Financial transactions (data type violations)

## 🤖 AI Features

### Anomaly Detection
- Statistical analysis using Z-scores
- Historical pattern learning
- Adaptive thresholds

### Root Cause Analysis
- Correlates multiple failures
- Provides business context
- Suggests remediation actions

### Natural Language Chatbot
- Query quality metrics
- Compare historical trends
- Get actionable recommendations

## 🔧 Configuration

### Adding New Data Sources

1. Create connector in `backend/connectors/`
2. Implement `BaseConnector` interface
3. Register in connector factory
4. Update UI dropdown

### Adding New Quality Checks

1. Create check in `dq_engine/checks/`
2. Create Airflow operator in `airflow/plugins/`
3. Add to DAG template
4. Update UI configuration

## 📈 Monitoring

- **Airflow UI**: Monitor DAG runs and task status
- **API Metrics**: `/api/health` endpoint
- **Database**: Query `quality_metrics` table for trends

## 🚨 Troubleshooting

### Services won't start
```bash
docker-compose down -v
docker-compose up -d --build
```

### Airflow DAG not appearing
```bash
docker-compose restart airflow-scheduler
```

### Database connection errors
Check `.env` file and ensure PostgreSQL is running

## 📝 License

MIT License

## 👥 Contributors

Built for Hackathon 2026

## 🙏 Acknowledgments

- Apache Airflow
- FastAPI
- Streamlit
- OpenAI
