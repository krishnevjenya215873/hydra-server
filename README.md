# CRYPTOSPREAD Server

Серверная часть приложения CRYPTOSPREAD для мониторинга спредов криптовалют.

## Архитектура

```
┌─────────────────────────────────────────────────────────────────┐
│                        CRYPTOSPREAD Server                       │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │   FastAPI   │  │  WebSocket  │  │     Price Worker        │  │
│  │   REST API  │  │   Manager   │  │  (постоянный опрос API) │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│         │                │                      │               │
│         └────────────────┼──────────────────────┘               │
│                          │                                      │
│                    ┌─────┴─────┐                                │
│                    │ PostgreSQL│                                │
│                    │ (Supabase)│                                │
│                    └───────────┘                                │
└─────────────────────────────────────────────────────────────────┘
                           │
            ┌──────────────┼──────────────┐
            │              │              │
      ┌─────┴─────┐  ┌─────┴─────┐  ┌─────┴─────┐
      │  Клиент 1 │  │  Клиент 2 │  │  Клиент N │
      │   (PyQt)  │  │   (PyQt)  │  │   (PyQt)  │
      └───────────┘  └───────────┘  └───────────┘
```

## Деплой на хостинг

### Вариант 1: Railway (рекомендуется)

1. Зайди на [railway.app](https://railway.app) и войди через GitHub
2. Нажми **New Project** → **Deploy from GitHub repo**
3. Выбери репозиторий с папкой `server`
4. Добавь переменную окружения:
   ```
   DATABASE_URL=postgresql://postgres.zsjmgnwzxyzmntpfgsau:MyPassword123@aws-1-eu-west-2.pooler.supabase.com:6543/postgres
   ```
5. Railway автоматически задеплоит сервер

### Вариант 2: Render

1. Зайди на [render.com](https://render.com)
2. Создай **New Web Service**
3. Подключи GitHub репозиторий
4. Настройки:
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `uvicorn main:app --host 0.0.0.0 --port $PORT`
5. Добавь переменную `DATABASE_URL`

### Вариант 3: Docker (VPS)

```bash
# Сборка образа
docker build -t cryptospread-server .

# Запуск
docker run -d -p 8000:8000 \
  -e DATABASE_URL="postgresql://..." \
  cryptospread-server
```

## Локальный запуск

```bash
# Установка зависимостей
pip install -r requirements.txt

# Установка переменной окружения
export DATABASE_URL="postgresql://postgres.zsjmgnwzxyzmntpfgsau:MyPassword123@aws-1-eu-west-2.pooler.supabase.com:6543/postgres"

# Запуск сервера
python run_server.py
```

## API Endpoints

### Публичные

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/health` | Проверка состояния сервера |
| GET | `/stats` | Статистика сервера |
| WS | `/ws` | WebSocket для реального времени |
| GET | `/api/tokens` | Список токенов |
| POST | `/api/tokens` | Добавить токен |
| GET | `/api/history/{token}` | История спредов |

### Админ (требуется авторизация)

| Метод | Путь | Описание |
|-------|------|----------|
| POST | `/api/admin/login` | Авторизация |
| GET | `/api/admin/proxies` | Список прокси |
| POST | `/api/admin/proxies/bulk` | Добавить прокси списком |
| DELETE | `/api/admin/tokens/{name}` | Удалить токен |
| DELETE | `/api/admin/proxies/{id}` | Удалить прокси |

## Админ-панель

Доступна по адресу: `http://your-server/admin`

**Учётные данные по умолчанию:**
- Логин: `admin`
- Пароль: `admin123`

### Функции:

1. **Прокси** — добавление списком в формате `login:pass@ip:port`, выбор SOCKS5/HTTP
2. **Токены** — просмотр и удаление (только админ может удалить с сервера)
3. **Статистика** — количество токенов, прокси, подключённых клиентов

## База данных (Supabase)

Используется PostgreSQL через Supabase.

### Таблицы

- `tokens` — токены для мониторинга
- `spread_history` — история спредов (2 дня)
- `proxies` — список прокси
- `admin_users` — администраторы
- `server_settings` — настройки сервера

### Просмотр данных

1. Зайди в [Supabase Dashboard](https://supabase.com/dashboard)
2. Выбери проект
3. Нажми **Table Editor** в левом меню
4. Выбери таблицу для просмотра

## Переменные окружения

| Переменная | Описание | Обязательно |
|------------|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string | Да |
| `PORT` | Порт сервера | Нет (default: 8000) |

## WebSocket API

### Подключение

```javascript
const ws = new WebSocket('wss://your-server.railway.app/ws');
```

### Сообщения от сервера

```json
// Начальные данные при подключении
{"type": "initial_data", "payload": {...}}

// Обновление данных
{"type": "data", "payload": {...}}
```

## Безопасность

- Токены авторизации действительны 24 часа
- Пароли хранятся в виде SHA256 хэша
- CORS разрешён для всех источников
