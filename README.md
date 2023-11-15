### Описание структуры приложения  



### Переменные окружения

`FLASK_APP_NAME`  
`DEBUG`  
`XLSX_INPUT_DIR`  
`XLSX_DIR_CHECK_INTERVAL_SEC`  

Реквизиты для подключения к СУБД PostgreSQL. (Используются как приложением так и файлом `docker-compose.yml`)  
`DB_USER`  
`DB_PASSWORD`  
`DB_NAME`  

### Запуск приложения

Установка зависимостей:

```
cd DeltaService/
python3 -m venv env
source ./env/bin/activate
pip install -r ./requirements.txt
```

Запуск:  

```
cd DeltaService/src/delta_service/
python3 app.py
```

### Миграции БД  

```
cd DeltaService/
```

Автогенерация миграций из метаданных SQLAlchemy: `alembic revision --autogenerate -m "<message>"`
Миграция на актуальную ревизию: `alembic upgrade head`
Миграция на ревизию вперед: `alembic upgrade +1`
Миграция на ревизию назад: `alembic upgrade -1`
