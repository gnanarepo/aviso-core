# Aviso Core - Django REST Framework

A Django REST Framework application for managing drilldown fields, hierarchies, and data processing.

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set Environment (Optional)

```bash
export DJANGO_ENVIRONMENT=development
export MONGO_DB_URL=mongodb://root:pass123@localhost:27017/
```

### 3. Run Migrations

```bash
python manage.py migrate
```

### 4. Start Development Server

```bash
python manage.py runserver
```

### 5. Test the API

```bash
curl -X POST "http://localhost:8000/api/v2/drilldown-fields/?period=2026Q1" \
  -H "Content-Type: application/json" \
  -d '{"fields_list": ["field1", "field2"]}'
```

## 📁 Project Structure

See [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md) for detailed structure documentation.

Key directories:

- `aviso_core/` - Django project configuration
- `api/` - REST API endpoints (versioned)
- `config/` - Configuration management
- `domainmodel/` - Domain models
- `utils/` - Utility functions
- `tasks/` - Background tasks (Celery)

## 📚 Documentation

- **[PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)** - Detailed project structure and organization
- **[MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)** - Migration guide from plain Django to DRF

## 🔧 Configuration

### Settings

Settings are organized by environment in `aviso_core/settings/`:

- `base.py` - Shared settings
- `development.py` - Development settings
- `production.py` - Production settings
- `testing.py` - Testing settings

Set `DJANGO_ENVIRONMENT` environment variable to switch between environments.

### Environment Variables

- `DJANGO_ENVIRONMENT` - Settings environment (development/production/testing)
- `DEBUG` - Debug mode (True/False)
- `SECRET_KEY` - Django secret key
- `MONGO_DB_URL` - MongoDB connection URL
- `MONGO_DB_NAME` - MongoDB database name
- `ALLOWED_HOSTS` - Comma-separated list of allowed hosts

## 📡 API Endpoints

### API Version 2

#### Drilldown Fields

- **Endpoint**: `POST /api/v2/drilldown-fields/`
- **Query Parameters**:
  - `period` (required, multiple): Period identifiers
  - `owner_mode` (optional): Boolean flag
  - `drilldown` (required if owner_mode=true): Drilldown identifier
- **Request Body** (if owner_mode=false):
  ```json
  {
    "fields_list": ["field1", "field2"]
  }
  ```

## 🔄 API Versioning

The API uses namespace-based versioning:

- `/api/v1/` - API version 1 (placeholder)
- `/api/v2/` - API version 2 (current)

## 🧪 Testing

Run tests using pytest:

```bash
pytest
```

Or use Django's test runner:

```bash
python manage.py test
```

## 🛠️ Development

### Adding New API Endpoints

1. Create view in `api/v2/views.py` or create a new view file
2. Create serializer in `api/v2/serializers.py`
3. Register route in `api/v2/urls.py`
4. Add tests in `tests/`

### Running with Docker

```bash
docker-compose up -d  # Start MongoDB
python manage.py runserver
```

## 📝 License

[Add your license here]

## 🤝 Contributing

[Add contribution guidelines here]

## 📞 Support

For issues or questions, please refer to the documentation files or create an issue.
