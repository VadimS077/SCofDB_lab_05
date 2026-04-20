-- wrk script: GET order card endpoint
-- Usage:
-- wrk -t4 -c100 -d30s -s loadtest/wrk/order_card.lua http://localhost:8082
--
-- TODO: перед запуском подставьте валидный order_id в path.
-- Реализация ниже позволяет не править файл: можно передать ORDER_ID через env.
local order_id = os.getenv("ORDER_ID") or "{{order_id}}"
local use_cache = os.getenv("USE_CACHE") or "true"

wrk.method = "GET"
wrk.path = "/api/cache-demo/orders/" .. order_id .. "/card?use_cache=" .. use_cache
