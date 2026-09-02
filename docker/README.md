# 本地可观测性栈

本目录的 `docker-compose.yml` 会启动 Alloy、Mimir、Loki、Tempo、Pyroscope 和 Grafana。后端采用单进程和本地文件存储，适合开发和远程测试，不适合生产环境。

进入本目录后启动服务：

```bash
cd docker
export GRAFANA_ADMIN_PASSWORD='replace-with-a-strong-password'
docker compose up -d
docker compose ps
```

## 常用 Compose 命令

```bash
# 创建或更新并在后台启动所有服务
docker compose up -d

# 查看服务状态
docker compose ps

# 持续查看所有服务日志
docker compose logs -f

# 只重启 Grafana
docker compose restart grafana

# 停止并删除容器，保留数据卷
docker compose down

# 停止并删除容器和数据卷
docker compose down -v
```

所有服务均使用 `network_mode: host`，直接共享宿主机网络栈，不再使用 Docker 端口映射和 Compose 服务名解析。远程 Spark 应将 `spark.telemetry.endpoint` 配置为 `http://<compose-host>:4317`，并将 `spark.telemetry.profile.endpoint` 配置为 `http://<compose-host>:9999`。部署到远程机器时应通过防火墙或安全组限制这些端口的访问来源。

| Component | Port |
|---|---|
| Grafana | `3000` |
| Alloy UI | `12345` |
| Alloy OTLP gRPC / HTTP | `4317` / `4318` |
| Alloy Pyroscope HTTP receiver | `9999` |
| Mimir | `9009` |
| Loki | `3100` |
| Tempo | `3200` |
| Pyroscope | `4040` |

为避免 host 网络模式下的监听冲突，Tempo 的后端 OTLP gRPC / HTTP 端口调整为 `14317` / `14318`；Spark 只连接 Alloy 的 OTLP gRPC `4317`。Mimir、Loki、Tempo、Pyroscope 的内部 gRPC 端口分别为 `19095`、`19096`、`19097`、`19098`。

Grafana 会自动 provision Mimir、Loki、Tempo 和 Pyroscope 数据源，并配置 metrics exemplar、trace 和 log 的跳转关系。Profile 由 Spark JVM 中的 Pyroscope Java Agent 推送到 Alloy `9999`，再由 Alloy 写入 Pyroscope `4040`。
