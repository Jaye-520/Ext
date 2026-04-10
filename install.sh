#!/bin/bash
set -e

###############################################################################
# Ext - 抖音字幕提取工具 安装脚本
###############################################################################

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 默认配置
MYSQL_HOST="localhost"
MYSQL_PORT="3306"
MYSQL_USER="root"
MYSQL_PASSWORD=""
MYSQL_DATABASE="media_crawler_pro"
REDIS_HOST="localhost"
REDIS_PORT="6379"
SCHEDULER_INTERVAL="1"
SUDO_PASSWORD=""

###############################################################################
# 工具函数
###############################################################################

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_banner() {
    echo -e "${CYAN}"
    echo "╔═══════════════════════════════════════════════════════════╗"
    echo "║                                                           ║"
    echo "║            Ext - 抖音字幕提取工具 安装脚本                  ║"
    echo "║                                                           ║"
    echo "╚═══════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

###############################################################################
# 检测函数
###############################################################################

check_sudo() {
    log_info "检测 sudo 权限..."
    
    if ! sudo -v 2>/dev/null; then
        log_error "需要 sudo 权限来安装系统包"
        exit 1
    fi
    
    log_success "sudo 权限正常"
}

check_python() {
    log_info "检测 Python 版本..."
    
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 未安装"
        exit 1
    fi
    
    PYTHON_VERSION=$(python3 -c 'import sys; print(sys.version_info[1])')
    PYTHON_MAJOR=$(python3 -c 'import sys; print(sys.version_info[0])')
    
    if [ "$PYTHON_MAJOR" -lt 3 ] || ([ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_VERSION" -lt 10 ]); then
        log_error "Python 版本需要 >= 3.10，当前版本: $(python3 --version)"
        exit 1
    fi
    
    log_success "Python 版本: $(python3 --version)"
}

check_uv() {
    log_info "检测 uv..."
    
    if command -v uv &> /dev/null; then
        log_success "uv 已安装: $(uv --version)"
        return 0
    fi
    
    log_warn "uv 未安装，正在安装..."
    
    # 下载并安装 uv
    curl -LsSf https://astral.sh/uv/install.sh | sh
    
    # 添加到 PATH
    export PATH="$HOME/.local/bin:$PATH"
    
    if command -v uv &> /dev/null; then
        log_success "uv 安装成功: $(uv --version)"
    else
        log_error "uv 安装失败"
        exit 1
    fi
}

check_redis() {
    log_info "检测 Redis..."
    
    if command -v redis-server &> /dev/null; then
        log_success "Redis 已安装: $(redis-server --version | head -1)"
        return 0
    fi
    
    log_warn "Redis 未安装，正在安装..."
    
    if [ -n "$SUDO_PASSWORD" ]; then
        echo "$SUDO_PASSWORD" | sudo -S apt update -qq
        echo "$SUDO_PASSWORD" | sudo -S apt install -y redis-server
    else
        sudo apt update -qq
        sudo apt install -y redis-server
    fi
    
    if command -v redis-server &> /dev/null; then
        log_success "Redis 安装成功"
    else
        log_error "Redis 安装失败"
        exit 1
    fi
}

check_ffmpeg() {
    log_info "检测 FFmpeg..."
    
    if command -v ffmpeg &> /dev/null; then
        log_success "FFmpeg 已安装: $(ffmpeg -version | head -1)"
        return 0
    fi
    
    log_warn "FFmpeg 未安装，正在安装..."
    
    if [ -n "$SUDO_PASSWORD" ]; then
        echo "$SUDO_PASSWORD" | sudo -S apt update -qq
        echo "$SUDO_PASSWORD" | sudo -S apt install -y ffmpeg
    else
        sudo apt update -qq
        sudo apt install -y ffmpeg
    fi
    
    if command -v ffmpeg &> /dev/null; then
        log_success "FFmpeg 安装成功"
    else
        log_error "FFmpeg 安装失败"
        exit 1
    fi
}

check_celery() {
    log_info "检测 Redis 服务..."
    
    if pgrep redis-server > /dev/null; then
        log_success "Redis 服务正在运行"
    else
        log_warn "Redis 服务未运行，正在启动..."
        redis-server --daemonize yes
        sleep 1
        if pgrep redis-server > /dev/null; then
            log_success "Redis 服务启动成功"
        else
            log_error "Redis 服务启动失败"
            exit 1
        fi
    fi
}

###############################################################################
# 交互配置
###############################################################################

input_config() {
    echo ""
    log_info "请配置数据库连接信息"
    echo ""
    
    # MySQL 主机
    read -p "MySQL 主机 [$MYSQL_HOST]: " INPUT
    MYSQL_HOST="${INPUT:-$MYSQL_HOST}"
    
    # MySQL 端口
    read -p "MySQL 端口 [$MYSQL_PORT]: " INPUT
    MYSQL_PORT="${INPUT:-$MYSQL_PORT}"
    
    # MySQL 用户
    read -p "MySQL 用户 [$MYSQL_USER]: " INPUT
    MYSQL_USER="${INPUT:-$MYSQL_USER}"
    
    # MySQL 密码
    echo -n "MySQL 密码: "
    read -s MYSQL_PASSWORD
    echo ""
    
    # MySQL 数据库
    read -p "数据库名 [$MYSQL_DATABASE]: " INPUT
    MYSQL_DATABASE="${INPUT:-$MYSQL_DATABASE}"
    
    # Redis 配置
    read -p "Redis 主机 [$REDIS_HOST]: " INPUT
    REDIS_HOST="${INPUT:-$REDIS_HOST}"
    
    read -p "Redis 端口 [$REDIS_PORT]: " INPUT
    REDIS_PORT="${INPUT:-$REDIS_PORT}"
    
    # 调度间隔
    read -p "调度间隔(分钟) [$SCHEDULER_INTERVAL]: " INPUT
    SCHEDULER_INTERVAL="${INPUT:-$SCHEDULER_INTERVAL}"
    
    echo ""
    log_info "配置完成"
    echo "  MySQL: $MYSQL_HOST:$MYSQL_PORT/$MYSQL_DATABASE"
    echo "  Redis: $REDIS_HOST:$REDIS_PORT"
    echo "  调度间隔: $SCHEDULER_INTERVAL 分钟"
    echo ""
}

input_sudo() {
    echo -n "请输入 sudo 密码(用于安装系统包): "
    read -s SUDO_PASSWORD
    echo ""
}

###############################################################################
# 生成配置
###############################################################################

generate_config() {
    log_info "生成 config.yaml..."
    
    cat > config.yaml << EOF
scheduler:
  interval_minutes: $SCHEDULER_INTERVAL
  batch_size: 10

worker:
  concurrency: 2

crawler_db:
  host: $MYSQL_HOST
  port: $MYSQL_PORT
  user: $MYSQL_USER
  password: '$MYSQL_PASSWORD'
  database: $MYSQL_DATABASE

result_db:
  host: $MYSQL_HOST
  port: $MYSQL_PORT
  user: $MYSQL_USER
  password: '$MYSQL_PASSWORD'
  database: $MYSQL_DATABASE

redis:
  host: $REDIS_HOST
  port: $REDIS_PORT

faster_whisper:
  model_size: base
  device: cpu
EOF
    
    log_success "config.yaml 生成完成"
}

###############################################################################
# 安装依赖
###############################################################################

install_dependencies() {
    log_info "安装 Python 依赖..."
    
    # 确保 uv 在 PATH 中
    export PATH="$HOME/.local/bin:$PATH"
    
    uv sync
    
    log_success "Python 依赖安装完成"
}

###############################################################################
# 初始化数据库
###############################################################################

init_database() {
    log_info "初始化数据库..."
    
    # 创建数据库(如果不存在)
    if [ -n "$SUDO_PASSWORD" ]; then
        echo "$SUDO_PASSWORD" | sudo -S mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" -e "CREATE DATABASE IF NOT EXISTS \`$MYSQL_DATABASE\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;" 2>/dev/null || true
    else
        mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" -e "CREATE DATABASE IF NOT EXISTS \`$MYSQL_DATABASE\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;" 2>/dev/null || true
    fi
    
    # 运行初始化脚本
    uv run python scripts/init_db.py
    
    log_success "数据库初始化完成"
}

###############################################################################
# 启动服务
###############################################################################

start_services() {
    log_info "启动服务..."
    echo ""
    log_info "使用以下命令启动:"
    echo "  cd $(pwd)"
    echo "  ./start.sh"
    echo ""
    
    read -p "是否现在启动? [Y/n]: " INPUT
    if [[ "$INPUT" =~ ^[Nn]$ ]]; then
        log_info "安装完成，再见!"
        exit 0
    fi
    
    echo ""
    log_info "启动中..."
    
    # 后台启动 worker
    uv run celery -A src.main worker --loglevel=info --concurrency=2 > logs/worker.log 2>&1 &
    WORKER_PID=$!
    
    # 等待 worker 启动
    sleep 3
    
    if ps -p $WORKER_PID > /dev/null 2>&1; then
        log_success "Worker 启动成功 (PID: $WORKER_PID)"
    else
        log_error "Worker 启动失败"
        exit 1
    fi
    
    # 启动 scheduler
    nohup uv run python -m src.main > logs/scheduler.log 2>&1 &
    SCHEDULER_PID=$!
    
    if ps -p $SCHEDULER_PID > /dev/null 2>&1; then
        log_success "Scheduler 启动成功 (PID: $SCHEDULER_PID)"
    else
        log_error "Scheduler 启动失败"
        exit 1
    fi
    
    echo ""
    log_success "服务启动完成!"
    echo ""
    log_info "日志位置: logs/"
    echo "  Worker 日志: logs/worker.log"
    echo "  Scheduler 日志: logs/scheduler.log"
    echo ""
    log_info "查看日志: tail -f logs/scheduler.log"
}

###############################################################################
# 主流程
###############################################################################

main() {
    print_banner
    
    # 1. 输入 sudo 密码
    input_sudo
    
    # 2. 输入配置
    input_config
    
    # 3. 检测系统依赖
    check_sudo
    check_python
    check_uv
    check_redis
    check_ffmpeg
    
    # 4. 确保 Redis 运行
    check_celery
    
    # 5. 生成配置
    generate_config
    
    # 6. 安装 Python 依赖
    install_dependencies
    
    # 7. 初始化数据库
    init_database
    
    # 8. 启动服务
    start_services
    
    echo ""
    log_success "安装完成!"
}

# 运行
main "$@"
