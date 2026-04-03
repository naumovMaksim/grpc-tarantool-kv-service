# grpc-tarantool-kv-service
## Описание

Это учебный backend-сервис на Java, который предоставляет gRPC API для работы с key-value хранилищем.

В качестве базы данных используется **Tarantool**.  
Ключ хранится как `string`, значение — как **nullable binary** (`varbinary`, может быть `null`).

Сервис поддерживает операции:

- `put(key, value)`
- `get(key)`
- `delete(key)`
- `range(key_since, key_to)`
- `count()`

## Основные возможности

- gRPC API
- хранение данных в Tarantool
- поддержка `null` для `value`
- валидация входных данных
- логгирование
- запуск через Docker Compose
- базовые unit/in-process тесты

## Технологии

- Java 21
- Maven
- gRPC Java
- Protocol Buffers
- Tarantool 3.2
- Docker / Docker Compose
- JUnit 5
- SLF4J Simple

---

## Архитектура проекта

```text
src/main/java/io/github/naumovmaksim/grpctarantoolkv
├── Application.java
├── grpc
│   └── KvServiceImpl.java
├── model
│   └── KvRecord.java
├── repository
│   ├── KvRepository.java
│   └── tarantool
│       ├── TarantoolKvRepository.java
│       └── TarantoolKvTuple.java
└── validation
    └── RequestValidator.java
```

## Подготовка проекта

1. Клонировать репозиторий
```
git clone https://github.com/naumovMaksim/grpc-tarantool-kv-service.git
cd grpc-tarantool-kv-service
```
2. Создать .env
```
cp .env.example .env
```

Скопировать .env.example в .env:

Можно просто создать .env вручную и вставить значения из .env.example.

## Основной способ запуска: Docker Compose

Это рекомендуемый способ запуска проекта.

Через compose.yaml запускаются 2 контейнера:

- app - Java gRPC сервис
- tarantool - база данных

Tarantool инициализируется автоматически через:
`docker/tarantool/init.lua`

В этом файле:
- создается пользователь app
- создается KV space
- задается формат:
  - `key: string`
  - `value: varbinary, nullable`
- создается primary index
  
### Команда запуска
```
docker compose up --build
```
  
Просмотр логов
```
docker compose logs -f
```
  
Просмотр статуса контейнеров
```
docker compose ps
```
  
Остановка
```
docker compose down
```
  
Остановка с удалением volume
```
docker compose down -v
```

## Локальный запуск без Docker
Приложение читает конфигурацию через `System.getenv()`.
При запуске из IDE переменные окружения нужно задать вручную в Run Configuration

1. Поднять Tarantool через Docker Compose
```
docker compose up -d tarantool
```
Tarantool запускается с конфигурацией из файла:
`docker/tarantool/init.lua`

2. В IntelliJ IDEA указать переменные окружения для Application
3. Запустить Application.java

## Примеры запросов для Postman
bytes в Postman нужно передавать в base64

Put с обычным значением
```
{
  "key": "a",
  "value": "c2Fk"
}
```
Put с null
```
{
  "key": "b"
}
```
Get
```
{  
  "key": "a"
}
```
Delete
```
{
  "key": "a"
}
```
Count - можно без тела

Range
```
{
  "key_since": "a",
  "key_to": "z"
}
```

### Ожидаемое поведение

get для существующего ключа
```
{
  "found": true,
  "key": "a",
  "value": "c2Fk"
}
```
get для ключа с null value
```
{
  "found": true,
  "key": "b"
}
```
get для отсутствующего ключа
```
{
  "found": false,
  "key": "missing"
}
```

## Валидация
Сервис проверяет входные данные.

Некорректные случаи:
- пустой key
- key только из пробелов
- пустой key_since
- пустой key_to
- key_since > key_to

Для некорректного запроса сервис возвращает:
INVALID_ARGUMENT

## Логгирование

В проекте настроено логгирование через slf4j-simple.

Файл конфигурации: `src/main/resources/simplelogger.properties`

## Тесты

В проекте есть базовые тесты:

- RequestValidatorTest
- KvServiceImplTest

Запуск тестов
```mvn test```

## Сборка проекта
Компиляция
```mvn clean compile```

Полная сборка
```mvn clean package```

После этого в target создается fat jar: `target/app.jar`

Его можно запустить так: ```java -jar target/app.jar```

При локальном запуске jar не забудь передать переменные окружения, если Tarantool поднят не с дефолтными параметрами.

## Docker-файлы
Dockerfile собирает Java-приложение в два этапа:

1. build stage — сборка jar через Maven
2. runtime stage — запуск готового app.jar

compose.yaml поднимает:

- приложение
- Tarantool
- volume для данных
- healthcheck для Tarantool

Автор

Maxim Naumov
